package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/obs"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// ProtocolVersion is the wire contract this build implements.
//
// It is versioned independently of the binary because an engine and an SDK WILL drift in the field.
// A handshake on connect costs an hour now and saves a support incident later (delivery model §7).
const ProtocolVersion = "cachet.v1"

// Cache is the subset of the cache client the engine needs.
//
// It is declared here, by the consumer, and has three methods rather than the client's full surface
// (CONTRIBUTING.md rule 10). That also makes the engine testable against a stub without pulling a
// cache server into a unit test.
type Cache interface {
	Get(ctx context.Context, key string) (cache.Entry, bool, error)
	Set(ctx context.Context, key string, e cache.Entry) error
}

// Options configures an Engine.
type Options struct {
	// Router maps keys to shards.
	Router *storage.Router
	// Shards holds an open connection per shard id in Router.
	Shards map[storage.ShardID]*storage.Shard

	// Cache is optional. A nil cache is the Phase 0 configuration: every read goes to the database,
	// which is the baseline every later row in the benchmark table is compared against.
	Cache Cache

	// MaxSessionShards caps the size of a session token.
	MaxSessionShards int

	// MaxClockSkew bounds the disagreement between engine and shard clocks. It shortens the
	// BOUNDED(t) window so the engine stays conservative about its own clock.
	MaxClockSkew time.Duration

	// Now supplies the current time. Injectable so the freshness rules can be tested against a
	// fixed instant rather than against the machine's clock.
	Now func() time.Time

	// Version is this build's version string, reported during the handshake.
	Version string

	// Metrics is optional. When nil, the engine records nothing — which is only appropriate in
	// tests; the binary always supplies one.
	Metrics *obs.Metrics

	Logger *slog.Logger
}

// Engine serves the Cachet data plane.
//
// In Phase 0 there is deliberately no cache anywhere in this type. Every read goes to the database.
// That is the baseline every later benchmark in this project is measured against, and it only means
// anything if it is the same code path with the cache removed rather than a separate program.
type Engine struct {
	cachetv1.UnimplementedCacheServiceServer

	router           *storage.Router
	shards           map[storage.ShardID]*storage.Shard
	cache            Cache
	maxSessionShards int
	maxClockSkew     time.Duration
	now              func() time.Time
	version          string
	log              *slog.Logger

	// metrics is optional; a nil Metrics is a no-op, so tests need not wire a registry.
	metrics *obs.Metrics
}

// New builds an Engine, verifying that every routable shard actually has a connection.
//
// A shard present in the routing topology but missing from the connection map would send a share of
// the key space to a nil handle. Catching that at construction makes it a boot failure instead of a
// panic on whichever request first hashes to the wrong place.
func New(opts Options) (*Engine, error) {
	if opts.Router == nil {
		return nil, errors.New("engine: nil router")
	}
	for _, id := range opts.Router.Shards() {
		if opts.Shards[id] == nil {
			return nil, fmt.Errorf("engine: shard %q is in the topology but has no connection", id)
		}
	}
	if len(opts.Shards) != len(opts.Router.Shards()) {
		return nil, errors.New("engine: shard connections do not match the routing topology")
	}

	maxShards := opts.MaxSessionShards
	if maxShards <= 0 {
		maxShards = consistency.DefaultMaxSessionShards
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	return &Engine{
		router:           opts.Router,
		shards:           opts.Shards,
		cache:            opts.Cache,
		maxSessionShards: maxShards,
		maxClockSkew:     opts.MaxClockSkew,
		now:              nowFn,
		version:          opts.Version,
		log:              log,
		metrics:          opts.Metrics,
	}, nil
}

// Handshake reports whether this server can serve the calling client.
func (e *Engine) Handshake(_ context.Context, req *cachetv1.HandshakeRequest) (*cachetv1.HandshakeResponse, error) {
	resp := &cachetv1.HandshakeResponse{
		ProtocolVersion: ProtocolVersion,
		ServerVersion:   e.version,
		Compatible:      true,
		SupportedLevels: []cachetv1.ConsistencyLevel{
			consistency.Strong.Proto(),
			consistency.Session.Proto(),
			consistency.Bounded.Proto(),
			consistency.Eventual.Proto(),
		},
	}

	// An empty protocol version means an older client that predates the handshake; it is accepted,
	// because refusing it would break exactly the clients the handshake exists to help.
	if v := req.GetProtocolVersion(); v != "" && v != ProtocolVersion {
		resp.Compatible = false
		resp.IncompatibilityReason = fmt.Sprintf(
			"client speaks %s, this server speaks %s", v, ProtocolVersion)
	}
	return resp, nil
}

// Get reads one row.
//
// Phase 0 has no cache, so every level is served from the database and the reported level is the
// one that was asked for — a direct read satisfies all four. The consistency plumbing is
// nevertheless exercised end to end from the first commit: the requirement is validated, the
// session watermark is advanced, and the response reports what was served. Adding that later would
// mean retrofitting it into a read path already shaped without it.
func (e *Engine) Get(ctx context.Context, req *cachetv1.GetRequest) (*cachetv1.GetResponse, error) {
	reqmt, err := consistency.RequirementFromProto(req.GetLevel(), req.GetStalenessBound())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key, err := ParseKey(req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	shard, id, err := e.shardFor(key.String())
	if err != nil {
		return nil, err
	}
	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)

	if entry, served := e.fromCache(ctx, reqmt, key.String(), id, token); served {
		token.Advance(string(id), entry.FillVersion)
		return &cachetv1.GetResponse{
			Found:   true,
			Record:  entryToProto(key.ID, entry),
			Meta:    cacheHitMeta(reqmt.Level, entry),
			Session: token.Proto(),
		}, nil
	}

	rec, fill, err := shard.Get(ctx, key.ID)
	e.metrics.RecordOriginRead()
	switch {
	case errors.Is(err, storage.ErrNotFound):
		// Absence is an answer, not an error: "this row does not exist" is a cacheable fact, and an
		// insert must later invalidate that negative entry.
		token.Advance(string(id), uint64(fill))
		return &cachetv1.GetResponse{
			Found:   false,
			Meta:    readMeta(reqmt.Level, 0, fill),
			Session: token.Proto(),
		}, nil
	case err != nil:
		return nil, e.rpcError(ctx, "get", err)
	}

	// Observing advances the watermark, which is what gives monotonic reads without any extra
	// state (CONSISTENCY.md §3.2).
	token.Advance(string(id), uint64(fill))
	e.fill(ctx, key.String(), rec, fill)

	return &cachetv1.GetResponse{
		Found:   true,
		Record:  recordToProto(rec),
		Meta:    readMeta(reqmt.Level, rec.Version, fill),
		Session: token.Proto(),
	}, nil
}

// fromCache attempts to serve a read from the cache.
//
// A cache failure is deliberately NOT an error: it degrades into a miss and the read falls through
// to the database. A cache that has stopped answering must not take the system down with it — but
// it is counted, because a silent fallback to the origin is exactly the failure that looks like a
// mysterious database load spike.
func (e *Engine) fromCache(
	ctx context.Context,
	req consistency.Requirement,
	key string,
	shardID storage.ShardID,
	token *consistency.Token,
) (cache.Entry, bool) {
	if e.cache == nil || req.Level.BypassesCache() {
		return cache.Entry{}, false
	}

	entry, hit, err := e.cache.Get(ctx, key)
	switch {
	case err != nil:
		e.metrics.RecordCacheOp("get", "error")
		e.log.WarnContext(ctx, "cache read failed; falling through to the origin", "key", key, "err", err)
		return cache.Entry{}, false
	case !hit:
		e.metrics.RecordCacheOp("get", "miss")
		return cache.Entry{}, false
	}

	watermark, known := token.Watermark(string(shardID))
	if !AcceptEntry(Freshness{
		Requirement:    req,
		FillVersion:    storage.Version(entry.FillVersion),
		Watermark:      watermark,
		WatermarkKnown: known,
		Now:            e.now(),
		MaxClockSkew:   e.maxClockSkew,
	}) {
		// The entry exists but is not fresh enough for the level that was asked for. That is a
		// stale-miss rather than a plain miss, and the two are counted separately: the ratio
		// between them is what says whether a level's cost is coming from cache capacity or from
		// the guarantee itself.
		e.metrics.RecordCacheOp("get", "stale")
		return cache.Entry{}, false
	}

	e.metrics.RecordCacheOp("get", "hit")
	return entry, true
}

// fill writes a freshly read row back to the cache.
//
// Failures are logged and counted but never returned: the caller already has the correct answer
// from the database, and failing their request because the cache write failed would turn a
// degradation into an outage.
func (e *Engine) fill(ctx context.Context, key string, rec storage.Record, fillVersion storage.Version) {
	if e.cache == nil {
		return
	}
	entry := cache.Entry{
		RowVersion:  uint64(rec.Version),
		FillVersion: uint64(fillVersion),
		Payload:     rec.Payload,
	}
	if err := e.cache.Set(ctx, key, entry); err != nil {
		e.metrics.RecordCacheOp("set", "error")
		e.log.WarnContext(ctx, "cache fill failed", "key", key, "err", err)
		return
	}
	e.metrics.RecordCacheOp("set", "ok")
}

// BatchGet reads several rows, one query per shard rather than one per key.
//
// There is deliberately no cross-key snapshot at any level: Cachet caches rows, not transactions.
// Offering one here would invite the assumption that it holds across shards, where it cannot
// (CONSISTENCY.md §6).
func (e *Engine) BatchGet(ctx context.Context, req *cachetv1.BatchGetRequest) (*cachetv1.BatchGetResponse, error) {
	reqmt, err := consistency.RequirementFromProto(req.GetLevel(), req.GetStalenessBound())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	parsed := make(map[string]Key, len(req.GetKeys()))
	for _, raw := range req.GetKeys() {
		k, err := ParseKey(raw)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		parsed[k.String()] = k
	}

	keys := make([]string, 0, len(parsed))
	for k := range parsed {
		keys = append(keys, k)
	}
	groups, err := e.router.Group(keys)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)
	out := make(map[string]*cachetv1.Record, len(keys))
	var newest storage.Version

	for shardID, shardKeys := range groups {
		ids := make([]uint64, 0, len(shardKeys))
		for _, k := range shardKeys {
			ids = append(ids, parsed[k].ID)
		}

		rows, fill, err := e.shards[shardID].BatchGet(ctx, ids)
		if err != nil {
			return nil, e.rpcError(ctx, "batch get", err)
		}
		token.Advance(string(shardID), uint64(fill))
		if fill > newest {
			newest = fill
		}
		for id, rec := range rows {
			out[Key{Table: entitiesTable, ID: id}.String()] = recordToProto(rec)
		}
	}

	return &cachetv1.BatchGetResponse{
		Records: out,
		Meta:    readMeta(reqmt.Level, 0, newest),
		Session: token.Proto(),
	}, nil
}

// Put writes one row and advances the caller's session watermark to the committed version.
//
// The watermark advance is what makes read-own-writes possible at all: the client carries it
// forward, and a later read rejects any cache entry filled before this write. Returning it is
// therefore part of the write's contract, not a convenience.
func (e *Engine) Put(ctx context.Context, req *cachetv1.PutRequest) (*cachetv1.PutResponse, error) {
	key, err := ParseKey(req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetRecord() == nil {
		return nil, status.Error(codes.InvalidArgument, "engine: put requires a record")
	}
	shard, id, err := e.shardFor(key.String())
	if err != nil {
		return nil, err
	}

	rec, err := recordFromProto(req.GetRecord())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rec.ID = key.ID // the key is authoritative; a mismatched record.id would write to the wrong row

	version, err := shard.Put(ctx, rec)
	if err != nil {
		return nil, e.rpcError(ctx, "put", err)
	}

	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)
	token.Advance(string(id), uint64(version))

	return &cachetv1.PutResponse{
		Meta:    &cachetv1.WriteMeta{Version: uint64(version)},
		Session: token.Proto(),
	}, nil
}

// Delete removes one row.
func (e *Engine) Delete(ctx context.Context, req *cachetv1.DeleteRequest) (*cachetv1.DeleteResponse, error) {
	key, err := ParseKey(req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	shard, id, err := e.shardFor(key.String())
	if err != nil {
		return nil, err
	}
	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)

	version, err := shard.Delete(ctx, key.ID)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		// Deleting an absent row is not an error, but the caller is told, because it determines
		// whether a negative cache entry needed invalidating.
		return &cachetv1.DeleteResponse{
			Existed: false,
			Meta:    &cachetv1.WriteMeta{},
			Session: token.Proto(),
		}, nil
	case err != nil:
		return nil, e.rpcError(ctx, "delete", err)
	}

	token.Advance(string(id), uint64(version))
	return &cachetv1.DeleteResponse{
		Existed: true,
		Meta:    &cachetv1.WriteMeta{Version: uint64(version)},
		Session: token.Proto(),
	}, nil
}

func (e *Engine) shardFor(key string) (*storage.Shard, storage.ShardID, error) {
	id, err := e.router.ShardFor(key)
	if err != nil {
		return nil, "", status.Error(codes.InvalidArgument, err.Error())
	}
	shard := e.shards[id]
	if shard == nil {
		return nil, "", status.Errorf(codes.Internal, "engine: no connection for shard %q", id)
	}
	return shard, id, nil
}

// rpcError maps a storage failure onto a gRPC status.
//
// Cancellation is reported as such rather than as an internal error: a client that walked away
// having its own deadline reported back as a server fault would make every latency investigation
// start in the wrong place.
func (e *Engine) rpcError(ctx context.Context, op string, err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	e.log.ErrorContext(ctx, "storage error", "op", op, "err", err)
	return status.Errorf(codes.Internal, "engine: %s failed", op)
}

func readMeta(level consistency.Level, rowVersion, fillVersion storage.Version) *cachetv1.ReadMeta {
	return &cachetv1.ReadMeta{
		LevelServed: level.Proto(),
		CacheHit:    false,
		RowVersion:  uint64(rowVersion),
		FillVersion: uint64(fillVersion),
	}
}

func cacheHitMeta(level consistency.Level, entry cache.Entry) *cachetv1.ReadMeta {
	return &cachetv1.ReadMeta{
		LevelServed: level.Proto(),
		CacheHit:    true,
		RowVersion:  entry.RowVersion,
		FillVersion: entry.FillVersion,
	}
}

func entryToProto(id uint64, entry cache.Entry) *cachetv1.Record {
	// tenant_id and status are not cached in Phase 1: nothing reads them on the hot path, and
	// widening the entry costs memory on every key to serve a field no caller uses. When a caller
	// needs them, they join the entry encoding with a version bump rather than being smuggled in.
	return &cachetv1.Record{
		Id:      id,
		Payload: entry.Payload,
		Version: entry.RowVersion,
	}
}

func recordToProto(r storage.Record) *cachetv1.Record {
	return &cachetv1.Record{
		Id:       r.ID,
		TenantId: r.TenantID,
		Status:   uint32(r.Status),
		Payload:  r.Payload,
		Version:  uint64(r.Version),
	}
}

// recordFromProto decodes a record from the wire.
//
// status is uint32 on the wire (proto3 has no uint8) and uint8 in the schema, so an out-of-range
// value has to be rejected here. Converting it silently would wrap — a client sending 300 would
// store 44 — and the row would then differ from what the caller believes it wrote, which is a
// consistency bug arriving through a type conversion rather than through the cache.
func recordFromProto(p *cachetv1.Record) (storage.Record, error) {
	st := p.GetStatus()
	if st > math.MaxUint8 {
		return storage.Record{}, fmt.Errorf("engine: status %d does not fit in a uint8", st)
	}
	return storage.Record{
		ID:       p.GetId(),
		TenantID: p.GetTenantId(),
		Status:   uint8(st),
		Payload:  p.GetPayload(),
	}, nil
}
