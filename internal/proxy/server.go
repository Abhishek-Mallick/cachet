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

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
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

	CachedTable string

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
	case opts.CachedTable == "":
		return nil, errors.New("proxy: no cached table")
	case opts.UpstreamAddr == "":
		return nil, errors.New("proxy: no upstream address")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("proxy: listen %s: %w", opts.Listen, err)
	}
	return &Server{opts: opts, log: opts.Logger, ln: ln, wire: server.NewDefaultServer()}, nil
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
	session *cachetv1.SessionToken
}

func (c *conn) UseDB(dbName string) error { return c.up.UseDB(dbName) }

func (c *conn) HandleQuery(query string) (*gomysql.Result, error) {
	plan := Classify(c.srv.opts.CachedTable, query)

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
		res, served, err := c.cachedRead(plan, false)
		if err != nil {
			c.srv.log.Warn("proxy: cached read failed, falling through to the database",
				"id", plan.ID, "err", err)
			break
		}
		if served {
			return res, nil
		}

	case PointWrite:
		return c.pointWrite(query, plan)

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
				c.srv.opts.CachedTable, firstWords(query))
		}
	}

	return c.up.Execute(query)
}

// cachedRead answers a point select from Cachet, returning served=false when the caller should ask
// the database instead.
func (c *conn) cachedRead(plan Plan, binary bool) (*gomysql.Result, bool, error) {
	resp, err := c.srv.opts.Engine.Get(c.ctx, &cachetv1.GetRequest{
		Key:     c.srv.opts.CachedTable + ":" + strconv.FormatUint(plan.ID, 10),
		Level:   cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
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

	row := make([]any, 0, len(plan.Columns))
	for _, col := range plan.Columns {
		v, ok := columnValue(plan.ID, resp.GetRecord(), col)
		if !ok {
			// Unreachable while the classifier and this switch agree on the cacheable set; if they
			// ever disagree, the database answers rather than the proxy guessing.
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
func (c *conn) pointWrite(query string, plan Plan) (*gomysql.Result, error) {
	key := c.srv.opts.CachedTable + ":" + strconv.FormatUint(plan.ID, 10)

	rewritten, ok := rewriteWithVersionBump(query)
	if !ok {
		return nil, fmt.Errorf("cachet proxy: could not maintain the version column for: %s", firstWords(query))
	}

	// The version the row will carry after the write. Read before, so a DELETE still has one.
	before, err := c.rowVersion(plan.ID)
	if err != nil {
		return nil, err
	}

	res, err := c.up.Execute(rewritten)
	if err != nil {
		return nil, err
	}

	// before+1 is what the rewritten statement set, and is strictly newer than any version the
	// cached entry can hold, so the tombstone cannot lose its compare-and-set.
	if err := c.invalidate(key, before+1); err != nil {
		// The write is committed. Reporting an error now would tell the caller their write failed
		// when it did not; the CDC tailer is the backstop for exactly this.
		c.srv.log.Warn("proxy: invalidation failed after a committed write; the tailer must catch it",
			"key", key, "err", err)
	}
	return res, nil
}

func (c *conn) rowVersion(id uint64) (uint64, error) {
	r, err := c.up.Execute(fmt.Sprintf(
		"SELECT version FROM `%s` WHERE id = %d", c.srv.opts.CachedTable, id))
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
	plan := Classify(c.srv.opts.CachedTable, query)

	upstreamSQL := query
	switch plan.Kind {
	case OpaqueWrite:
		if c.srv.opts.OpaqueWrites == RefuseOpaqueWrites {
			return 0, 0, nil, fmt.Errorf(
				"cachet proxy: refusing a write to %q it cannot resolve to specific rows: %s. "+
					"Rewrite it as a single-row statement, use the Cachet SDK, or set "+
					"opaque_writes=forward if this application maintains the version column itself",
				c.srv.opts.CachedTable, firstWords(query))
		}
	case PointWrite:
		rewritten, ok := rewriteWithVersionBump(query)
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

	id, known := p.plan.boundID(args)

	switch {
	case p.plan.Kind == PointSelect && known && !c.inTx:
		plan := p.plan
		plan.ID = id
		// binary: a prepared statement's rows go back in the binary protocol. Encoding them as
		// text produces "malformed packet" at the client, which says nothing about the cause.
		res, served, err := c.cachedRead(plan, true)
		if err != nil {
			c.srv.log.Warn("proxy: cached read failed, falling through to the database",
				"id", id, "err", err)
			break
		}
		if served {
			return res, nil
		}

	case p.plan.Kind == PointWrite && known:
		return c.executePreparedWrite(p, args, id)
	}

	return p.stmt.Execute(args...)
}

// executePreparedWrite runs an already-rewritten write and invalidates the row it changed.
func (c *conn) executePreparedWrite(p *prepared, args []any, id uint64) (*gomysql.Result, error) {
	key := c.srv.opts.CachedTable + ":" + strconv.FormatUint(id, 10)

	before, err := c.rowVersion(id)
	if err != nil {
		return nil, err
	}

	res, err := p.stmt.Execute(args...)
	if err != nil {
		return nil, err
	}

	if err := c.invalidate(key, before+1); err != nil {
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

// columnValue maps a cached record onto one column of the table.
func columnValue(id uint64, rec *cachetv1.Record, col string) (any, bool) {
	switch col {
	case "id":
		return id, true
	case "tenant_id":
		return uint64(rec.GetTenantId()), true
	case "status":
		return uint64(rec.GetStatus()), true
	case "payload":
		return rec.GetPayload(), true
	case "version":
		return rec.GetVersion(), true
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
