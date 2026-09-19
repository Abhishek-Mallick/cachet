// Package cachet is the Go client for Cachet.
//
// The SDK is mandatory, and that is a feature rather than a limitation (delivery model §4). Cachet's
// session guarantee is carried by a token, and a token nobody propagates is a guarantee nobody has.
// An application talking raw gRPC would have to remember to thread a watermark through every call
// and every service hop; the ones it forgot would not fail, they would quietly return staler data
// than the level they asked for. This client carries the token from its first release precisely so
// that "I forgot" is not a reachable state (O-303).
//
// A Client is safe for concurrent use. The session lives in the Client, not in the connection, which
// is why a dropped connection or a failed-over engine costs nothing: engines are stateless with
// respect to sessions (CONSISTENCY.md §4).
package cachet

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// protocolVersion is the contract this client was built against.
const protocolVersion = "cachet.v1"

// Version is the client version reported during the handshake.
var Version = "dev"

// Client is a connection to Cachet, plus the session it has accumulated.
type Client struct {
	conn *grpc.ClientConn
	api  cachetv1.CacheServiceClient

	defaultLevel consistency.Level
	maxShards    int

	mu      sync.Mutex
	session map[string]uint64
}

// Option configures a Client.
type Option func(*options)

type options struct {
	defaultLevel  consistency.Level
	maxShards     int
	dialOptions   []grpc.DialOption
	skipHandshake bool
}

// WithDefaultLevel sets the consistency level used when a call does not name one.
func WithDefaultLevel(l consistency.Level) Option {
	return func(o *options) { o.defaultLevel = l }
}

// WithMaxSessionShards caps how many shard watermarks the client keeps before evicting the oldest.
func WithMaxSessionShards(n int) Option {
	return func(o *options) { o.maxShards = n }
}

// WithDialOptions passes gRPC dial options through.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dialOptions = append(o.dialOptions, opts...) }
}

// WithoutHandshake skips the compatibility check on connect.
//
// It exists for tooling that must attach to a server it may not fully understand. Applications
// should not use it: the handshake is what turns an incompatibility into a startup error instead of
// a puzzling failure on whichever request first touches the field that changed.
func WithoutHandshake() Option {
	return func(o *options) { o.skipHandshake = true }
}

// normalizeTarget accepts the address syntax the engine's own configuration uses.
//
// A listener in cachet.yaml is written "tcp://host:port" or "unix:///path". gRPC understands the
// second and not the first, so pasting a TCP listener address into Dial used to fail deep inside
// the resolver with "too many colons in address" — an error that names nothing the caller did.
// Stripping the scheme here costs one line and removes a stumbling block from the first thirty
// seconds of using this SDK.
//
// Everything else is passed through untouched, including unix:// and explicit gRPC schemes like
// dns:///, so this can only widen what Dial accepts.
func normalizeTarget(target string) string {
	return strings.TrimPrefix(target, "tcp://")
}

// Dial connects to a Cachet engine and verifies it can serve this client.
//
// The handshake happens here rather than lazily, so an incompatible server is a startup failure. A
// client that discovered the mismatch on its first request would surface it as a confusing error in
// application code that has nothing to do with the cause.
func Dial(ctx context.Context, target string, opts ...Option) (*Client, error) {
	o := options{defaultLevel: consistency.Session, maxShards: consistency.DefaultMaxSessionShards}
	for _, fn := range opts {
		fn(&o)
	}
	if o.maxShards <= 0 {
		return nil, errors.New("cachet: max session shards must be positive")
	}

	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, o.dialOptions...)

	conn, err := grpc.NewClient(normalizeTarget(target), dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("cachet: dial %s: %w", target, err)
	}

	c := &Client{
		conn:         conn,
		api:          cachetv1.NewCacheServiceClient(conn),
		defaultLevel: o.defaultLevel,
		maxShards:    o.maxShards,
		session:      map[string]uint64{},
	}

	if !o.skipHandshake {
		if err := c.handshake(ctx); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Client) handshake(ctx context.Context) error {
	resp, err := c.api.Handshake(ctx, &cachetv1.HandshakeRequest{
		ProtocolVersion: protocolVersion,
		ClientVersion:   Version,
	})
	if err != nil {
		return fmt.Errorf("cachet: handshake: %w", err)
	}
	if !resp.GetCompatible() {
		return fmt.Errorf("cachet: server %s cannot serve protocol %s: %s",
			resp.GetServerVersion(), protocolVersion, resp.GetIncompatibilityReason())
	}
	return nil
}

// Close releases the connection.
func (c *Client) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("cachet: close: %w", err)
	}
	return nil
}

// Session returns a snapshot of the client's watermarks.
//
// Hand it to ContextWithSession to carry the guarantee across a service boundary, or inspect it to
// see which shards this session has touched.
func (c *Client) Session() map[string]uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copySession(c.session)
}

// AdoptSession merges watermarks into the client's session.
//
// A downstream service calls this with what it extracted from baggage, and inherits the upstream's
// guarantee. Merging rather than replacing, because the downstream's own writes matter too and a
// replace would silently discard them.
func (c *Client) AdoptSession(watermarks map[string]uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session = c.capped(MergeSessions(c.session, watermarks))
}

// sessionFor builds the token to send with a request.
//
// A session on the CONTEXT wins over the client's own, because an explicit per-request session is
// how a server handles work on behalf of many callers without leaking one caller's guarantee into
// another's request.
func (c *Client) sessionFor(ctx context.Context) map[string]uint64 {
	if w, ok := SessionFromContext(ctx); ok {
		return w
	}
	if w, ok := ExtractSession(ctx); ok {
		return w
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return copySession(c.session)
}

// observe folds a response's watermarks back into the client's session.
func (c *Client) observe(w map[string]uint64) {
	if len(w) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session = c.capped(MergeSessions(c.session, w))
}

// capped enforces the shard cap by dropping the lowest watermarks.
//
// Dropping the LOWEST rather than the oldest: a low watermark is the one whose loss costs least,
// because the entries it would have rejected are the ones most likely to have been superseded
// anyway. Either choice loses a guarantee, and the response's degraded flag is what tells the caller
// it happened (CONSISTENCY.md §4).
func (c *Client) capped(w map[string]uint64) map[string]uint64 {
	if len(w) <= c.maxShards {
		return w
	}
	for len(w) > c.maxShards {
		var lowestShard string
		lowest := uint64(math.MaxUint64)
		for shard, v := range w {
			if v < lowest {
				lowest, lowestShard = v, shard
			}
		}
		delete(w, lowestShard)
	}
	return w
}

// ─── reads ──────────────────────────────────────────────────────────────────────

// ReadOption tunes a single read.
type ReadOption func(*readOptions)

type readOptions struct {
	level consistency.Level
	bound time.Duration
	set   bool
}

// AtLevel overrides the client's default consistency level for one call.
func AtLevel(l consistency.Level) ReadOption {
	return func(o *readOptions) { o.level, o.set = l, true }
}

// WithinStaleness requests BOUNDED(d). It implies the BOUNDED level.
func WithinStaleness(d time.Duration) ReadOption {
	return func(o *readOptions) { o.level, o.bound, o.set = consistency.Bounded, d, true }
}

// Record is one row.
type Record struct {
	Key      string
	TenantID uint32
	Status   uint8
	Payload  []byte
	Version  uint64
}

// ReadMeta reports what actually happened, as distinct from what was asked for.
type ReadMeta struct {
	// LevelServed may be weaker than the level requested.
	LevelServed consistency.Level

	// Degraded reports that the engine could not honour the requested level. Ignoring it is your
	// decision, but it must be a VISIBLE decision — a silent downgrade of a stated guarantee is the
	// failure Cachet exists to eliminate, so the SDK surfaces it as a field rather than a log line.
	Degraded       bool
	DegradedReason string

	CacheHit bool

	// RowVersion orders writes against each other; FillVersion answers freshness. Both are exposed
	// because they answer different questions (CONSISTENCY.md §1).
	RowVersion  uint64
	FillVersion uint64
}

// ReadResult is the answer to a read.
type ReadResult struct {
	// Found is false when the row does not exist, which is an answer rather than an error: absence
	// is a cacheable fact.
	Found  bool
	Record Record
	Meta   ReadMeta
}

// Get reads one row.
func (c *Client) Get(ctx context.Context, key string, opts ...ReadOption) (ReadResult, error) {
	ro := c.readOptions(opts)
	req := &cachetv1.GetRequest{
		Key:     key,
		Level:   ro.level.Proto(),
		Session: &cachetv1.SessionToken{Watermarks: c.sessionFor(ctx)},
	}
	if ro.level == consistency.Bounded {
		req.StalenessBound = durationpb.New(ro.bound)
	}

	resp, err := c.api.Get(ctx, req)
	if err != nil {
		return ReadResult{}, fmt.Errorf("cachet: get %s: %w", key, err)
	}
	c.observe(resp.GetSession().GetWatermarks())

	out := ReadResult{Found: resp.GetFound(), Meta: readMeta(resp.GetMeta())}
	if resp.GetFound() {
		out.Record = recordFromProto(key, resp.GetRecord())
	}
	return out, nil
}

// BatchGet reads several rows in one call.
//
// It is N independent reads, not a snapshot: two keys may reflect different instants, at every
// level. Cachet caches rows, not transactions (CONSISTENCY.md §6).
func (c *Client) BatchGet(ctx context.Context, keys []string, opts ...ReadOption) (map[string]Record, ReadMeta, error) {
	ro := c.readOptions(opts)
	req := &cachetv1.BatchGetRequest{
		Keys:    keys,
		Level:   ro.level.Proto(),
		Session: &cachetv1.SessionToken{Watermarks: c.sessionFor(ctx)},
	}
	if ro.level == consistency.Bounded {
		req.StalenessBound = durationpb.New(ro.bound)
	}

	resp, err := c.api.BatchGet(ctx, req)
	if err != nil {
		return nil, ReadMeta{}, fmt.Errorf("cachet: batch get: %w", err)
	}
	c.observe(resp.GetSession().GetWatermarks())

	out := make(map[string]Record, len(resp.GetRecords()))
	for key, rec := range resp.GetRecords() {
		out[key] = recordFromProto(key, rec)
	}
	return out, readMeta(resp.GetMeta()), nil
}

func (c *Client) readOptions(opts []ReadOption) readOptions {
	ro := readOptions{level: c.defaultLevel}
	for _, fn := range opts {
		fn(&ro)
	}
	return ro
}

// ─── writes ─────────────────────────────────────────────────────────────────────

// WriteResult is what a write did.
type WriteResult struct {
	Version uint64

	// Degraded reports that exact invalidation was abandoned; the write still committed. Degraded
	// describes the INVALIDATION, never the durability (CONSISTENCY.md §5).
	Degraded       bool
	DegradedReason string

	// EffectiveStalenessBound is what other sessions are promised for the affected keys until CDC
	// catches up.
	EffectiveStalenessBound time.Duration
}

// Put writes one row.
func (c *Client) Put(ctx context.Context, key string, rec Record) (WriteResult, error) {
	resp, err := c.api.Put(ctx, &cachetv1.PutRequest{
		Key: key,
		Record: &cachetv1.Record{
			TenantId: rec.TenantID,
			Status:   uint32(rec.Status),
			Payload:  rec.Payload,
		},
		Session: &cachetv1.SessionToken{Watermarks: c.sessionFor(ctx)},
	})
	if err != nil {
		return WriteResult{}, fmt.Errorf("cachet: put %s: %w", key, err)
	}
	c.observe(resp.GetSession().GetWatermarks())
	return writeResult(resp.GetMeta()), nil
}

// Delete removes one row. The boolean reports whether it existed, which is not an error either way.
func (c *Client) Delete(ctx context.Context, key string) (bool, WriteResult, error) {
	resp, err := c.api.Delete(ctx, &cachetv1.DeleteRequest{
		Key:     key,
		Session: &cachetv1.SessionToken{Watermarks: c.sessionFor(ctx)},
	})
	if err != nil {
		return false, WriteResult{}, fmt.Errorf("cachet: delete %s: %w", key, err)
	}
	c.observe(resp.GetSession().GetWatermarks())
	return resp.GetExisted(), writeResult(resp.GetMeta()), nil
}

// Predicate selects rows for a conditional write.
type Predicate struct {
	TenantID    uint32
	MatchStatus uint8
	SetStatus   uint8
}

// UpdateResult is what a conditional write did.
type UpdateResult struct {
	WriteResult

	// Matched is the blast radius, reported whether or not the keys were resolved exactly.
	Matched uint64

	// AffectedKeys is exact when Degraded is false and EMPTY when it is true — never partial. A
	// partial list would leave you unable to tell which rows still need treating as stale.
	AffectedKeys []string
}

// UpdateWhere applies a conditional write.
func (c *Client) UpdateWhere(ctx context.Context, p Predicate) (UpdateResult, error) {
	resp, err := c.api.UpdateWhere(ctx, &cachetv1.UpdateWhereRequest{
		TenantId:    p.TenantID,
		MatchStatus: uint32(p.MatchStatus),
		SetStatus:   uint32(p.SetStatus),
		Session:     &cachetv1.SessionToken{Watermarks: c.sessionFor(ctx)},
	})
	if err != nil {
		return UpdateResult{}, fmt.Errorf("cachet: update where: %w", err)
	}
	c.observe(resp.GetSession().GetWatermarks())

	return UpdateResult{
		WriteResult:  writeResult(resp.GetMeta()),
		Matched:      resp.GetMatched(),
		AffectedKeys: resp.GetAffectedKeys(),
	}, nil
}

// ─── conversions ────────────────────────────────────────────────────────────────

func readMeta(m *cachetv1.ReadMeta) ReadMeta {
	// A level the client cannot name falls back to the default rather than failing the read. The
	// caller already has its answer; refusing to hand it over because the METADATA used an enum
	// value from a newer server would turn a successful read into an error for no benefit.
	served, err := consistency.LevelFromProto(m.GetLevelServed())
	if err != nil {
		served = consistency.Session
	}
	return ReadMeta{
		LevelServed:    served,
		Degraded:       m.GetDegraded(),
		DegradedReason: m.GetDegradedReason(),
		CacheHit:       m.GetCacheHit(),
		RowVersion:     m.GetRowVersion(),
		FillVersion:    m.GetFillVersion(),
	}
}

func writeResult(m *cachetv1.WriteMeta) WriteResult {
	return WriteResult{
		Version:                 m.GetVersion(),
		Degraded:                m.GetDegraded(),
		DegradedReason:          m.GetDegradedReason(),
		EffectiveStalenessBound: m.GetEffectiveStalenessBound().AsDuration(),
	}
}

// recordFromProto decodes a record from the wire.
//
// status is uint32 on the wire (proto3 has no uint8) and uint8 in the schema. An out-of-range value
// is clamped to zero rather than wrapped: wrapping would hand the application a status it never
// stored — 300 arriving as 44 — which is a data corruption the caller has no way to detect. A server
// cannot produce one, since the engine rejects it on the way in, so this is a guard against a future
// schema widening rather than a case in flight today.
func recordFromProto(key string, r *cachetv1.Record) Record {
	status := r.GetStatus()
	if status > math.MaxUint8 {
		status = 0
	}
	return Record{
		Key:      key,
		TenantID: r.GetTenantId(),
		Status:   uint8(status),
		Payload:  r.GetPayload(),
		Version:  r.GetVersion(),
	}
}
