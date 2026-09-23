package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// OpaqueWritePolicy decides what happens to a write against the cached table that the classifier
// cannot pin to specific rows.
type OpaqueWritePolicy int

const (
	// RefuseOpaqueWrites rejects the statement with an error naming the problem. The default, and
	// the honest one: forwarding it would leave cache entries no invalidation can reach.
	RefuseOpaqueWrites OpaqueWritePolicy = iota

	// ForwardOpaqueWrites sends it upstream anyway. Correct ONLY where the application maintains
	// the version column itself, so the CDC tailer's invalidation carries a newer version than the
	// cached entry and is therefore applied rather than rejected.
	ForwardOpaqueWrites
)

// Options configures the proxy.
type Options struct {
	Listen string

	// Upstream is the real database, reached per client connection.
	UpstreamAddr     string
	UpstreamUser     string
	UpstreamPassword string
	UpstreamDB       string

	// User and Password are what clients present to the PROXY. Separate from the upstream
	// credentials on purpose: a proxy that required the database password from every client would
	// be a credential-distribution problem wearing a cache.
	User     string
	Password string

	// Table is the declared shape of the cached table. Every pattern this proxy matches and every
	// statement it builds comes from it, so a deployment caching its own table changes this and
	// nothing else.
	Table *schema.Descriptor

	// WholeTable reports that Table declares every column the live table has, which is what makes
	// `SELECT *` answerable from a cache entry. It is a boot fact the caller establishes against
	// INFORMATION_SCHEMA — see storage.UndeclaredColumns — not a guess.
	WholeTable bool

	Engine *engine.Engine
	Cache  *cache.Client

	OpaqueWrites OpaqueWritePolicy
	Logger       *slog.Logger
}

// Server speaks the MySQL wire protocol on behalf of Cachet.
type Server struct {
	opts Options
	log  *slog.Logger
	ln   net.Listener

	// matcher holds the compiled patterns for the cached table, built once at boot.
	matcher *Matcher

	// api is the generic-row protocol. The proxy reads rows positionally against the descriptor
	// rather than through the fixture-shaped v1 record, which is what lets it answer an arbitrary
	// column list for an arbitrary table.
	api *engine.V2

	// wire carries the protocol defaults (capabilities, charset, auth plugin) shared by every
	// connection this proxy accepts.
	wire *server.Server
}

// New validates options and binds the listener.
func New(opts Options) (*Server, error) {
	switch {
	case opts.Engine == nil:
		return nil, errors.New("proxy: no engine")
	case opts.Cache == nil:
		return nil, errors.New("proxy: no cache client")
	case opts.Table == nil:
		return nil, errors.New("proxy: no cached table")
	case opts.UpstreamAddr == "":
		return nil, errors.New("proxy: no upstream address")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	matcher, err := NewMatcher(opts.Table, opts.WholeTable)
	if err != nil {
		return nil, err
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("proxy: listen %s: %w", opts.Listen, err)
	}
	return &Server{
		opts:    opts,
		log:     opts.Logger,
		ln:      ln,
		matcher: matcher,
		api:     engine.NewV2(opts.Engine),
		wire:    server.NewDefaultServer(),
	}, nil
}

// Addr is where the proxy is listening, useful when Listen asked for port 0.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		_ = s.ln.Close()
	}()

	var wg sync.WaitGroup
	for {
		c, err := s.ln.Accept()
		if err != nil {
			// A closed listener during shutdown is the expected path, not a failure: Serve returns
			// ctx.Err() so a caller can still distinguish cancellation from a clean stop.
			if ctx.Err() != nil {
				wg.Wait()
				return ctx.Err()
			}
			return fmt.Errorf("proxy: accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handle(ctx, c)
		}()
	}
}

func (s *Server) handle(ctx context.Context, netConn net.Conn) {
	defer func() { _ = netConn.Close() }()

	up, err := client.Connect(s.opts.UpstreamAddr, s.opts.UpstreamUser, s.opts.UpstreamPassword, s.opts.UpstreamDB)
	if err != nil {
		s.log.Error("proxy: upstream connect", "err", err)
		return
	}
	defer func() { _ = up.Close() }()

	h := &conn{srv: s, up: up, ctx: ctx}
	c, err := s.wire.NewConn(netConn, s.opts.User, s.opts.Password, h)
	if err != nil {
		s.log.Debug("proxy: client handshake", "err", err)
		return
	}
	for ctx.Err() == nil {
		if err := c.HandleCommand(); err != nil {
			return
		}
	}
}

// conn is one client connection. A connection IS a session here: a bare SQL client has nowhere to
// hold a session token, and per-connection is the same scope MySQL itself gives a caller.
type conn struct {
	srv  *Server
	up   *client.Conn
	ctx  context.Context
	inTx bool

	// session is this connection's watermark, carried between statements exactly as the SDK
	// carries one for an application.
	session *cachetv2.SessionToken
}

func (c *conn) UseDB(dbName string) error { return c.up.UseDB(dbName) }

func (c *conn) HandleQuery(query string) (*gomysql.Result, error) {
	plan := c.srv.matcher.Classify(query)

	switch {
	case plan.BeginsTransaction:
		c.inTx = true
	case plan.EndsTransaction:
		c.inTx = false
	}

	switch plan.Kind {
	case PointSelect:
		// Inside a transaction the cache is not consulted at all. A transaction may have already
		// written rows it is about to read, and those writes are not visible to anyone else — the
		// cache included. Serving one from the cache would contradict the transaction's own view.
		if c.inTx {
			break
		}
		key, known := c.srv.matcher.Key(plan, nil)
		if !known {
			break
		}
		res, served, err := c.cachedRead(key, plan, false)
		if err != nil {
			c.srv.log.Warn("proxy: cached read failed, falling through to the database",
				"key", key, "err", err)
			break
		}
		if served {
			return res, nil
		}

	case PointWrite:
		key, known := c.srv.matcher.Key(plan, nil)
		if !known {
			break
		}
		return c.pointWrite(query, key)

	case OpaqueWrite:
		if c.srv.opts.OpaqueWrites == RefuseOpaqueWrites {
			// Refusing is the honest answer. Forwarding would let the write land with the version
			// column unchanged, so the tailer's invalidation would carry a version no newer than
			// the cached entry and be rejected by the compare-and-set — leaving a stale entry that
			// nothing will ever clear. A clear error beats silent staleness.
			return nil, fmt.Errorf(
				"cachet proxy: refusing a write to %q it cannot resolve to specific rows: %s. "+
					"Rewrite it as a single-row statement, use the Cachet SDK, or set "+
					"opaque_writes=forward if this application maintains the version column itself",
				c.srv.opts.Table.Name, firstWords(query))
		}
	}

	return c.up.Execute(query)
}

// cachedRead answers a point select from Cachet, returning served=false when the caller should ask
// the database instead.
//
// The row comes back as positional values against the descriptor, so the column list the caller
// asked for is resolved by index. There is no per-column switch to keep in step with a schema —
// the one that used to live here named the fixture's five columns and could not have served
// anybody else's table.
func (c *conn) cachedRead(key schema.Key, plan Plan, binary bool) (*gomysql.Result, bool, error) {
	resp, err := c.srv.api.Get(c.ctx, &cachetv2.GetRequest{
		Key:     key.String(),
		Level:   cachetv2.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
		Session: c.session,
	})
	if err != nil {
		return nil, false, err
	}
	c.session = resp.GetSession()

	if !resp.GetFound() {
		// An absent row is a real answer, and an empty result set is how SQL says it.
		rs, err := gomysql.BuildSimpleResultset(plan.Columns, nil, binary)
		if err != nil {
			return nil, false, err
		}
		return gomysql.NewResult(rs), true, nil
	}

	values := resp.GetRow().GetValues()
	if len(values) != len(c.srv.opts.Table.Columns) {
		// Unreachable while the engine and this proxy hold the same descriptor. If they ever stop
		// agreeing, the database answers rather than the proxy guessing at which column is which.
		return nil, false, nil
	}

	row := make([]any, 0, len(plan.Columns))
	for _, name := range plan.Columns {
		col := c.srv.opts.Table.Column(name)
		if col == nil {
			return nil, false, nil
		}
		v, ok := columnValue(col, values[col.Index])
		if !ok {
			return nil, false, nil
		}
		row = append(row, v)
	}

	rs, err := gomysql.BuildSimpleResultset(plan.Columns, [][]any{row}, binary)
	if err != nil {
		return nil, false, err
	}
	return gomysql.NewResult(rs), true, nil
}

// pointWrite forwards a single-row write and invalidates exactly the row it changed.
//
// The rewrite is what makes this correct. Cachet's invalidation is a versioned compare-and-set, so
// a tombstone carrying a version no newer than the cached entry is REJECTED — meaning a raw SQL
// write that leaves the version column alone would leave a stale entry that nothing ever clears.
// Bumping it in the statement itself keeps the discipline the SDK keeps, without asking the
// application to know about it.
func (c *conn) pointWrite(query string, key schema.Key) (*gomysql.Result, error) {
	rewritten, ok := c.srv.matcher.rewriteWithVersionBump(query)
	if !ok {
		return nil, fmt.Errorf("cachet proxy: could not maintain the version column for: %s", firstWords(query))
	}

	// The version the row will carry after the write. Read before, so a DELETE still has one.
	before, err := c.rowVersion(key)
	if err != nil {
		return nil, err
	}

	res, err := c.up.Execute(rewritten)
	if err != nil {
		return nil, err
	}

	// before+1 is what the rewritten statement set, and is strictly newer than any version the
	// cached entry can hold, so the tombstone cannot lose its compare-and-set.
	if err := c.invalidate(key.String(), before+1); err != nil {
		// The write is committed. Reporting an error now would tell the caller their write failed
		// when it did not; the CDC tailer is the backstop for exactly this.
		c.srv.log.Warn("proxy: invalidation failed after a committed write; the tailer must catch it",
			"key", key, "err", err)
	}
	return res, nil
}

// rowVersion reads the version a row holds before a write changes it.
//
// The statement is the descriptor's own read-by-primary-key, with the key values bound rather than
// formatted into the SQL. The previous version formatted an integer id into the text, which was
// safe only because an id was the one thing this proxy could parse; a string primary key would
// have made it a concatenation.
func (c *conn) rowVersion(key schema.Key) (uint64, error) {
	args := make([]any, len(key.Values))
	for i, v := range key.Values {
		args[i] = v
	}
	r, err := c.up.Execute(c.srv.matcher.versionStmt, args...)
	if err != nil {
		return 0, err
	}
	if r.Resultset == nil || len(r.Values) == 0 {
		return 0, nil
	}
	return r.Values[0][0].AsUint64(), nil
}

func (c *conn) invalidate(key string, version uint64) error {
	_, err := c.srv.opts.Cache.Tombstone(c.ctx, key, version)
	return err
}

func (c *conn) HandleFieldList(table string, fieldWildcard string) ([]*gomysql.Field, error) {
	return c.up.FieldList(table, fieldWildcard)
}

// prepared is one COM_STMT_PREPARE, and what the proxy decided about it.
//
// The plan is computed ONCE, here, from the statement text — which is the only time the text is
// available. Re-deriving it per execution would be a second classifier and a second way to be
// wrong about what a statement means.
type prepared struct {
	stmt *client.Stmt
	plan Plan
}

// HandleStmtPrepare classifies the template and applies the same rules the text path applies.
//
// This is not an optimisation. go-sql-driver — and most drivers — turn any query given arguments
// into a prepared statement, so this is how most applications write. A proxy that forwarded them
// untouched would let writes reach the database without their version bump, and every invalidation
// for those rows would lose its compare-and-set. The refusal has to hold here too, or it is
// bypassed by adding an argument.
func (c *conn) HandleStmtPrepare(query string) (int, int, any, error) {
	plan := c.srv.matcher.Classify(query)

	upstreamSQL := query
	switch plan.Kind {
	case OpaqueWrite:
		if c.srv.opts.OpaqueWrites == RefuseOpaqueWrites {
			return 0, 0, nil, fmt.Errorf(
				"cachet proxy: refusing a write to %q it cannot resolve to specific rows: %s. "+
					"Rewrite it as a single-row statement, use the Cachet SDK, or set "+
					"opaque_writes=forward if this application maintains the version column itself",
				c.srv.opts.Table.Name, firstWords(query))
		}
	case PointWrite:
		rewritten, ok := c.srv.matcher.rewriteWithVersionBump(query)
		if !ok {
			return 0, 0, nil, fmt.Errorf(
				"cachet proxy: could not maintain the version column for: %s", firstWords(query))
		}
		// The rewrite adds `version = version + 1`, which introduces no placeholder, so every
		// argument index the client will send is unchanged.
		upstreamSQL = rewritten
	}

	stmt, err := c.up.Prepare(upstreamSQL)
	if err != nil {
		return 0, 0, nil, err
	}
	return stmt.ParamNum(), stmt.ColumnNum(), &prepared{stmt: stmt, plan: plan}, nil
}

func (c *conn) HandleStmtExecute(ctx any, _ string, args []any) (*gomysql.Result, error) {
	p, ok := ctx.(*prepared)
	if !ok {
		return nil, errors.New("cachet proxy: unknown prepared statement")
	}

	key, known := c.srv.matcher.Key(p.plan, args)

	switch {
	case p.plan.Kind == PointSelect && known && !c.inTx:
		// binary: a prepared statement's rows go back in the binary protocol. Encoding them as
		// text produces "malformed packet" at the client, which says nothing about the cause.
		res, served, err := c.cachedRead(key, p.plan, true)
		if err != nil {
			c.srv.log.Warn("proxy: cached read failed, falling through to the database",
				"key", key, "err", err)
			break
		}
		if served {
			return res, nil
		}

	case p.plan.Kind == PointWrite && known:
		return c.executePreparedWrite(p, args, key)
	}

	return p.stmt.Execute(args...)
}

// executePreparedWrite runs an already-rewritten write and invalidates the row it changed.
func (c *conn) executePreparedWrite(p *prepared, args []any, key schema.Key) (*gomysql.Result, error) {
	before, err := c.rowVersion(key)
	if err != nil {
		return nil, err
	}

	res, err := p.stmt.Execute(args...)
	if err != nil {
		return nil, err
	}

	if err := c.invalidate(key.String(), before+1); err != nil {
		// The write is committed. Reporting an error now would say the write failed when it did
		// not; the CDC tailer is the backstop for exactly this.
		c.srv.log.Warn("proxy: invalidation failed after a committed write; the tailer must catch it",
			"key", key, "err", err)
	}
	return res, nil
}

func (c *conn) HandleStmtClose(ctx any) error {
	p, ok := ctx.(*prepared)
	if !ok {
		return nil
	}
	return p.stmt.Close()
}

func (c *conn) HandleOtherCommand(cmd byte, _ []byte) error {
	return gomysql.NewError(gomysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("cachet proxy: command %d is not supported", cmd))
}

// columnValue renders one cached column as the Go value the MySQL wire encoder expects.
//
// The type comes from the declaration, so the column the client sees has the same type it would
// have had from the database. Returning the canonical bytes for everything would be simpler and
// would change an integer column into a string one on the wire, which a client reading it into an
// int in the binary protocol would experience as a malformed packet.
//
// A NULL becomes an untyped nil, which the encoder sends as SQL NULL.
func columnValue(col *schema.Column, v *cachetv2.Value) (any, bool) {
	if v.GetIsNull() {
		if !col.Nullable {
			// The entry disagrees with the declaration. The database answers instead.
			return nil, false
		}
		return nil, true
	}

	text := string(v.GetData())
	switch col.Type {
	case schema.Uint8, schema.Uint32, schema.Uint64:
		u, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return nil, false
		}
		return u, true
	case schema.Int64:
		i, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, false
		}
		return i, true
	case schema.String, schema.Bytes, schema.Text:
		return v.GetData(), true
	default:
		return nil, false
	}
}

func firstWords(q string) string {
	if len(q) > 80 {
		return q[:80] + "…"
	}
	return q
}
