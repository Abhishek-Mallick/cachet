package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/admission"
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
// It is declared here, by the consumer, and names three operations rather than the client's full
// surface (CONTRIBUTING.md rule 10). That also makes the engine testable against a stub without
// pulling a cache server into a unit test.
//
// Fill and Tombstone both report whether they won their compare-and-set. The engine records the
// answer rather than discarding it: a rising rate of rejected fills means reads are consistently
// losing to writes on the same keys, which is a real condition with a real cause, and it is
// invisible if the boolean is dropped.
type Cache interface {
	Get(ctx context.Context, key string) (cache.Entry, bool, error)
	Fill(ctx context.Context, key string, e cache.Entry) (bool, error)
	Tombstone(ctx context.Context, key string, version uint64) (bool, error)

	// GetOrLease reads an entry or takes the exclusive right to fill it, in one round trip. It is
	// what bounds origin load per key: without it, every concurrent miss becomes an origin read.
	GetOrLease(ctx context.Context, key string) (cache.LeaseResult, error)

	// FillWithLease fills and hands the lease back in the same operation, so no window exists in
	// which the value is present but the lease is still held.
	FillWithLease(ctx context.Context, key string, e cache.Entry, token string) (bool, error)
}

// Options configures an Engine.
type Options struct {
	// Router maps keys to shards.
	// Shards holds an open connection per shard id in Router.
	Shards map[storage.ShardID]*storage.Shard

	// Tables are the declared tables this engine serves, in declaration order, each naming the
	// shards it lives on.
	//
	// Required, and with no default. A built-in table would be a default that is wrong for
	// everyone but this project's own fixtures, and the cacheable set is an operator's decision
	// with consistency consequences (ADR 0005).
	Tables []TableSpec

	// Cache is optional. A nil cache is the Phase 0 configuration: every read goes to the database,
	// which is the baseline every later row in the benchmark table is compared against.
	Cache Cache

	// MaxSessionShards caps the size of a session token.
	MaxSessionShards int

	// MaxAffectedKeys is where a conditional write stops resolving affected keys exactly and falls
	// back to CDC invalidation, reporting degraded=true. It is a guarantee setting: raising it buys
	// exactness at the cost of holding a transaction open across more row locks, and lowering it
	// trades staleness for write latency (CONSISTENCY.md §5).
	MaxAffectedKeys int

	// CDCLagBound is the staleness bound a degraded write promises other sessions until the tailer
	// catches up. It is reported to the caller as effective_staleness_bound, so a caller can decide
	// what to do about the weakening instead of discovering it later.
	CDCLagBound time.Duration

	// MaxClockSkew bounds the disagreement between engine and shard clocks. It shortens the
	// BOUNDED(t) window so the engine stays conservative about its own clock.
	MaxClockSkew time.Duration

	// SynchronousInvalidation makes writes tombstone the cache after commit and before the ack.
	// With it off, invalidation falls entirely to the CDC tailer — see config.Consistency.
	SynchronousInvalidation bool

	// Admission decides which keys are worth caching, from their observed read:write ratio. Nil
	// means cache everything, which is the configuration every benchmark row before this one was
	// measured with.
	Admission *admission.Controller

	// Leases bounds origin load per key. The zero value disables waiting entirely: a caller told
	// another fill is in progress goes straight to the database rather than waiting for it.
	Leases WaitPolicy

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

	// tables is what this engine serves, by name, each with its own ring over the shards it was
	// declared on; tableOrder is the order it was declared in, which is the order the handshake
	// publishes and an operator reads in a log line.
	tables     map[string]*table
	tableOrder []string

	cache            Cache
	maxSessionShards int
	maxAffectedKeys  int
	leases           WaitPolicy
	admission        *admission.Controller
	cdcLagBound      time.Duration
	maxClockSkew     time.Duration
	syncInvalidation bool
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
	tables, order, err := buildTables(opts.Tables, opts.Shards)
	if err != nil {
		return nil, err
	}

	maxAffected := opts.MaxAffectedKeys
	if maxAffected <= 0 {
		// A zero budget would degrade every conditional write, silently turning exact invalidation
		// off across the whole system. Defaulting is right for a test that does not care; accepting
		// an explicit zero would not be.
		maxAffected = consistency.DefaultMaxAffectedKeys
	}
	cdcLag := opts.CDCLagBound
	if cdcLag <= 0 {
		cdcLag = consistency.DefaultCDCLagBound
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
		tables:           tables,
		tableOrder:       order,
		cache:            opts.Cache,
		maxSessionShards: maxShards,
		maxAffectedKeys:  maxAffected,
		leases:           opts.Leases,
		admission:        opts.Admission,
		cdcLagBound:      cdcLag,
		maxClockSkew:     opts.MaxClockSkew,
		syncInvalidation: opts.SynchronousInvalidation,
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

// fromCache attempts to serve a read from the cache.
//
// A cache failure is deliberately NOT an error: it degrades into a miss and the read falls through
// to the database. A cache that has stopped answering must not take the system down with it — but
// it is counted, because a silent fallback to the origin is exactly the failure that looks like a
// mysterious database load spike.
func (e *Engine) fromCacheOrLease(
	ctx context.Context,
	req consistency.Requirement,
	key string,
	shardID storage.ShardID,
	token *consistency.Token,
) (cache.Entry, bool, string) {
	if e.cache == nil || req.Level.BypassesCache() {
		return cache.Entry{}, false, ""
	}

	// The read is counted whether or not the key is cacheable. That ordering is what makes
	// admission reversible: a key that was evicted still accumulates reads, so when its ratio
	// recovers it can be admitted again. Counting only admitted keys would make eviction a one-way
	// ratchet, and the first bad hour would cost a key its hit rate permanently.
	if e.admission != nil {
		e.admission.RecordRead(key)
		if !e.admission.ShouldCache(key) {
			e.metrics.RecordCacheOp("get", "not_admitted")
			return cache.Entry{}, false, ""
		}
	}

	entry, hit, lease := e.readWaitingForAnyFill(ctx, key)
	if !hit {
		return cache.Entry{}, false, lease
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

		// A stale hit still has to go to the origin, so it needs a lease for the refill exactly as a
		// miss does. Without one, a hot key whose watermark has just moved — which is every hot key
		// immediately after it is written — sends every concurrent reader to the database at once.
		return cache.Entry{}, false, e.leaseForRefill(ctx, key)
	}

	e.metrics.RecordCacheOp("get", "hit")
	return entry, true, ""
}

// readWaitingForAnyFill reads the cache, waiting briefly if another caller is already filling.
//
// Returns the lease token when this caller has been made responsible for the fill, and an empty
// token otherwise — including when the wait was exhausted. A caller that waited and gave up still
// reads the origin: it is served either way, and the only thing it loses is the chance to have been
// served from someone else's fill.
func (e *Engine) readWaitingForAnyFill(ctx context.Context, key string) (cache.Entry, bool, string) {
	for attempt := 0; ; attempt++ {
		res, err := e.cache.GetOrLease(ctx, key)
		if err != nil {
			e.metrics.RecordCacheOp("get", "error")
			e.log.WarnContext(ctx, "cache read failed; falling through to the origin", "key", key, "err", err)
			return cache.Entry{}, false, ""
		}

		switch res.Outcome {
		case cache.LeaseHit:
			return res.Entry, true, ""
		case cache.LeaseGranted:
			e.metrics.RecordCacheOp("get", "miss")
			e.metrics.RecordLease("granted")
			return cache.Entry{}, false, res.Token
		}

		// LeaseWait: somebody else is filling this key.
		if attempt >= e.leases.MaxAttempts() {
			// Bounded, always. A holder that died is indistinguishable from one that is nearly
			// finished, so waiting longer is a guess — and guessing wrong on the hottest key in the
			// system is a self-inflicted outage. Read the origin instead.
			e.metrics.RecordCacheOp("get", "miss")
			e.metrics.RecordLease("wait_exhausted")
			return cache.Entry{}, false, ""
		}

		select {
		case <-ctx.Done():
			e.metrics.RecordLease("wait_cancelled")
			return cache.Entry{}, false, ""
		case <-time.After(e.leases.Backoff(attempt)):
		}
		e.metrics.RecordLease("waited")
	}
}

// leaseForRefill takes a lease for a refill that a freshness rejection made necessary.
//
// A failure here is not an error: the refill proceeds without a lease, which costs admission
// control for this one key and nothing else. The compare-and-set still protects correctness.
func (e *Engine) leaseForRefill(ctx context.Context, key string) string {
	res, err := e.cache.GetOrLease(ctx, key)
	if err != nil || res.Outcome != cache.LeaseGranted {
		return ""
	}
	e.metrics.RecordLease("granted")
	return res.Token
}

// fill writes a freshly read row back to the cache.
//
// Failures are logged and counted but never returned: the caller already has the correct answer
// from the database, and failing their request because the cache write failed would turn a
// degradation into an outage.
// fillNegative caches the fact that a row does not exist.
//
// Absence is a cacheable fact, and caching it is what stops a workload probing for missing keys from
// bypassing the cache entirely. It is only safe because an insert invalidates the negative entry
// through the same compare-and-set as any other write, which is what gives read-own-inserts.
func (e *Engine) fillNegative(ctx context.Context, key string, fillVersion storage.Version) {
	e.fillNegativeHoldingLease(ctx, key, fillVersion, "")
}

// fillNegativeHoldingLease is fillNegative by a caller holding the key's lease.
//
// Absence is filled under a lease exactly like a value: a key that does not exist is just as
// capable of being stampeded as one that does, and a workload probing for missing rows is the case
// negative caching was built for in the first place.
func (e *Engine) fillNegativeHoldingLease(ctx context.Context, key string, fillVersion storage.Version, lease string) {
	if e.cache == nil {
		return
	}
	e.applyFill(ctx, key, cache.Entry{
		// A negative entry has no row version of its own — no row was read. The fill version is
		// what dates it, and it is the fill version every freshness rule consults anyway.
		FillVersion: uint64(fillVersion),
		Negative:    true,
	})
}

func (e *Engine) applyFill(ctx context.Context, key string, entry cache.Entry) {
	e.applyFillHoldingLease(ctx, key, entry, "")
}

// applyFillHoldingLease writes the entry and releases the lease in the same operation.
func (e *Engine) applyFillHoldingLease(ctx context.Context, key string, entry cache.Entry, lease string) {
	applied, err := e.cache.FillWithLease(ctx, key, entry, lease)
	switch {
	case err != nil:
		// The caller already holds the correct answer from the database. Failing their request
		// because a cache write failed would turn a degradation into an outage.
		e.metrics.RecordCacheOp("fill", "error")
		e.log.WarnContext(ctx, "cache fill failed", "key", key, "err", err)
	case applied:
		e.metrics.RecordCacheOp("fill", "applied")
	default:
		// Losing the compare-and-set is correct behaviour, not a failure: something newer is
		// already there. It is counted because a sustained rise means reads are consistently racing
		// writes on the same keys.
		e.metrics.RecordCacheOp("fill", "rejected")
	}
}

// invalidate tombstones a key at the version of the write that changed it.
//
// It runs AFTER the database commit and BEFORE the client's ack. That ordering is the whole
// mechanism behind read-own-writes for other processes: by the time the caller holds the ack, the
// stale entry is already invalidated at that version (CONSISTENCY.md §3.2).
func (e *Engine) invalidate(ctx context.Context, key string, version storage.Version) {
	// Counted before the early return, and outside the synchronous-invalidation check. A write is a
	// write whichever path invalidates it, and a key whose writes were only counted when
	// synchronous invalidation happened to be on would look read-heavy to admission precisely in
	// the configuration where caching it costs most.
	if e.admission != nil {
		e.admission.RecordWrite(key)
	}

	if e.cache == nil || !e.syncInvalidation {
		return
	}
	applied, err := e.cache.Tombstone(ctx, key, uint64(version))
	switch {
	case err != nil:
		// The write is already committed and durable; the row is correct in the database. What is
		// lost is the synchronous invalidation, so the key falls back to CDC — bounded by the
		// tailer's lag rather than immediate. That is a degradation to report, not a reason to fail
		// a committed write.
		e.metrics.RecordCacheOp("tombstone", "error")
		e.log.WarnContext(ctx, "invalidation failed; falling back to CDC for this key",
			"key", key, "version", uint64(version), "err", err)
	case applied:
		e.metrics.RecordCacheOp("tombstone", "applied")
	default:
		e.metrics.RecordCacheOp("tombstone", "rejected")
	}
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
	case errors.Is(err, ErrInvalidRequest):
		// The caller asked something this engine cannot answer — an undeclared table, a key that
		// does not fit its columns, a row of the wrong shape. Reported with the reason, because it
		// is the caller who can fix it; an Internal here would send them to read server logs for a
		// mistake they made.
		return status.Error(codes.InvalidArgument, err.Error())
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
