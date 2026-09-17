package cache

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Abhishek-Mallick/cachet/internal/breaker"
)

// The scripts are embedded rather than inlined as Go string literals so they can be linted, diffed
// and reviewed as Lua. They are the only place the compare-and-set invariant is actually enforced,
// which makes them the most important few lines in the project.
var (
	//go:embed lua/fill_cas.lua
	fillCASSource string
	//go:embed lua/tombstone_cas.lua
	tombstoneCASSource string
	//go:embed lua/read.lua
	readSource string
	//go:embed lua/read_lease.lua
	readLeaseSource string
)

// go-redis's Script wrapper tries EVALSHA first and falls back to EVAL on a NOSCRIPT reply, so a
// cache server that restarts and loses its script cache recovers without a round trip per call.
var (
	fillCAS      = redis.NewScript(fillCASSource)
	tombstoneCAS = redis.NewScript(tombstoneCASSource)
	readScript   = redis.NewScript(readSource)
	readLease    = redis.NewScript(readLeaseSource)
)

// Options configures a Client.
type Options struct {
	// Addresses are the cache servers. Valkey is the default and Redis is supported; they speak the
	// same protocol and Lua (ADR 0002).
	Addresses []string

	// TTL bounds how long an entry may live.
	//
	// In Phase 1 this is the ONLY thing bounding staleness, which is exactly what makes Phase 1's
	// staleness number bad enough to motivate Phase 2. From Phase 2 onward it becomes a backstop
	// behind exact invalidation rather than the strategy.
	TTL time.Duration

	// Timeout bounds a single cache operation. A cache that stops answering must degrade into a
	// miss quickly rather than adding its own latency to the database's.
	Timeout time.Duration

	// Breaker configures the per-node proportional circuit breaker. The zero value gets
	// DefaultBreaker.
	Breaker breaker.Options

	// LeaseTTL bounds how long one caller may hold the right to fill a key.
	//
	// It is a ceiling on damage, not a tuning knob: a holder that dies mid-fill stops blocking the
	// key after this long. Too short and two callers fill the same key, which costs an extra origin
	// read and nothing else, since the compare-and-set sorts out which value wins. Too long and a
	// dead holder stalls a hot key for that entire interval. Erring short is therefore the cheaper
	// mistake, which is why the default is close to a slow database read rather than to a timeout.
	LeaseTTL time.Duration
}

// DefaultLeaseTTL is the lease interval used when none is configured.
const DefaultLeaseTTL = 2 * time.Second

// DefaultBreaker is the breaker configuration a Client uses when none is supplied.
//
// A 10-second window is short enough to notice a node going bad within a handful of requests and
// long enough that a single slow second does not start shedding. MinRequests=20 is the evidence
// floor; FailureFloor=5% is the error rate every healthy node has anyway.
func DefaultBreaker() breaker.Options {
	return breaker.Options{
		Window:       10 * time.Second,
		Buckets:      10,
		MinRequests:  20,
		FailureFloor: 0.05,
		MaxShed:      0.95,
	}
}

// Client is Cachet's cache-side data path.
//
// It holds one connection pool per cache node and routes every key through its own Router, which is
// independent of database shard routing (see Router). A Client is safe for concurrent use: the pool
// map and the router are built once in New and never mutated, so no lock guards the read path.
type Client struct {
	router   *Router
	pools    map[string]*redis.Client
	breakers *breaker.Group
	ttl      time.Duration
	leaseTTL time.Duration
}

// New connects to every cache node and verifies each one answers.
//
// Failing at boot beats discovering on the first user request that a node was never reachable — and
// it must be EVERY node, not the first one that responds. A client that started with two of three
// nodes down would silently route a third of the key space into errors, present it as a collapsed
// hit rate, and give no indication that the cause was a node that never came up.
//
// Runtime unhealth is a different problem with a different answer: that is the circuit breaker's
// job, not a reason to refuse to boot.
func New(ctx context.Context, opts Options) (*Client, error) {
	if len(opts.Addresses) == 0 {
		return nil, errors.New("cache: no addresses configured")
	}
	if opts.TTL <= 0 {
		// A zero TTL means "never expire" in Redis. Since the TTL is Phase 1's only bound on
		// staleness, accepting zero would silently turn a bounded cache into an unbounded one.
		return nil, fmt.Errorf("cache: ttl must be positive, got %s", opts.TTL)
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 250 * time.Millisecond
	}

	router, err := NewRouter(opts.Addresses)
	if err != nil {
		return nil, err
	}

	bopts := opts.Breaker
	if bopts.Window == 0 && bopts.Buckets == 0 {
		bopts = DefaultBreaker()
	}
	breakers, err := breaker.NewGroup(bopts)
	if err != nil {
		return nil, err
	}

	leaseTTL := opts.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}

	c := &Client{
		router:   router,
		pools:    make(map[string]*redis.Client, len(router.Nodes())),
		breakers: breakers,
		ttl:      opts.TTL,
		leaseTTL: leaseTTL,
	}
	for _, addr := range router.Nodes() {
		rdb := redis.NewClient(&redis.Options{
			Addr: addr,

			// Bounded by the SAME timeout as reads and writes. It used to be a hardcoded two
			// seconds, which quietly made Options.Timeout's promise — "bounds a single cache
			// operation" — false: an operation could spend far longer than the configured budget
			// establishing a connection it was never going to get.
			//
			// This matters most exactly where the cache is supposed to help. The circuit breaker
			// exists to stop paying timeouts to a node that will not answer; an unbounded dial
			// undercuts the saving and hides it, because nothing measures the part that overran.
			DialTimeout:  timeout,
			ReadTimeout:  timeout,
			WriteTimeout: timeout,

			// The breaker owns retry policy, so the driver must not have one of its own. With
			// go-redis' default of three retries, a single Get against a dead node costs several
			// dial timeouts back to back, and the breaker still records it as ONE failure — so the
			// shed rate is computed from a cost model that understates reality by a factor of four.
			MaxRetries: -1,

			PoolSize: 64,
		})
		if err := pingWithin(ctx, rdb, bootProbeBudget); err != nil {
			_ = rdb.Close()
			_ = c.Close()
			return nil, fmt.Errorf("cache: ping %s: %w", addr, err)
		}
		c.pools[addr] = rdb
	}
	return c, nil
}

// bootProbeBudget is how long New will keep trying to reach one node before giving up on it.
//
// Boot and the request path want opposite things from a retry. A request must fail fast and fall
// through to the database; a boot probe against a cache that is still warming up should not turn a
// slow start into a failed deployment. So the dial stays bounded by the operation timeout and this
// budget bounds the RETRYING instead — which is what keeps a mistyped address from presenting as a
// thirty-second hang.
const bootProbeBudget = 2 * time.Second

// pingWithin retries a ping until it succeeds or the budget expires, returning the last error.
func pingWithin(ctx context.Context, rdb *redis.Client, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	var err error
	for {
		if err = rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// poolFor returns the connection pool for the node that owns key, and the node's name.
func (c *Client) poolFor(key string) (*redis.Client, string, error) {
	node, err := c.router.NodeFor(key)
	if err != nil {
		return nil, "", err
	}
	rdb, ok := c.pools[node]
	if !ok {
		// Unreachable while the router and the pool map are built together from one node list, and
		// worth an explicit error rather than a nil dereference if that ever stops being true.
		return nil, "", fmt.Errorf("cache: no connection pool for node %s", node)
	}
	return rdb, node, nil
}

// BreakerStats reports what the breaker has observed per node, so an operator can be told which
// node is being shed and why.
func (c *Client) BreakerStats() map[string]breaker.Stats { return c.breakers.Nodes() }

// Nodes returns the cache nodes this client is connected to, in sorted order.
func (c *Client) Nodes() []string { return c.router.Nodes() }

// NodeFor returns the cache node that owns key, so operators can answer "where does this live?"
// without reimplementing the ring.
func (c *Client) NodeFor(key string) (string, error) { return c.router.NodeFor(key) }

// Get reads one entry.
//
// A miss is reported as hit=false with no error, because a miss is the normal path rather than a
// failure — returning an error would make every cold read look like a fault and drown the ones that
// matter. A tombstoned entry reads as a miss: the marker is invisible to readers and exists only to
// make a late fill lose its compare-and-set.
func (c *Client) Get(ctx context.Context, key string) (Entry, bool, error) {
	rdb, node, err := c.poolFor(key)
	if err != nil {
		return Entry{}, false, err
	}

	b := c.breakers.For(node)
	if !b.Allow() {
		// Shed: report a miss without touching the node. The read falls through to the database,
		// which costs hit rate and saves the timeout this call was going to spend failing.
		return Entry{}, false, nil
	}

	res, err := readScript.Run(ctx, rdb, []string{key}).Slice()
	switch {
	case errors.Is(err, redis.Nil):
		// A miss is a healthy answer. Counting it as a failure would shed traffic to a node whose
		// only crime is holding keys nobody has filled yet.
		b.Success()
		return Entry{}, false, nil
	case err != nil:
		b.Failure()
		// The error is returned, not folded into a miss. The engine degrades it to a miss and falls
		// through to the database, but it also LOGS it and counts it as
		// cache_operations_total{op=get,result=error}. Swallowing it here would make a node outage
		// indistinguishable from a cold cache in every dashboard — the breaker would be shedding
		// traffic for a reason nobody could see.
		return Entry{}, false, fmt.Errorf("cache: get %s: %w", key, err)
	}
	b.Success()
	if len(res) == 0 {
		return Entry{}, false, nil
	}
	if len(res) != 6 {
		return Entry{}, false, fmt.Errorf("cache: get %s: %w: %d fields", key, ErrCorruptEntry, len(res))
	}

	entry, err := entryFromLua(res)
	if err != nil {
		// Corruption is surfaced rather than folded into a miss: a systematic encoding bug would
		// otherwise present itself as a mysterious drop in hit rate that nobody could explain.
		return Entry{}, false, fmt.Errorf("cache: get %s: %w", key, err)
	}
	return entry, true, nil
}

// Fill writes an entry if it wins the compare-and-set, reporting whether it was applied.
//
// The boolean is not incidental. The ratio of rejected to applied fills is how a racing read path
// becomes visible: a healthy system rejects a few, and a sudden rise means reads are consistently
// losing to writes on the same keys.
func (c *Client) Fill(ctx context.Context, key string, e Entry) (bool, error) {
	return c.FillWithLease(ctx, key, e, "")
}

// FillWithLease is Fill by a caller holding a lease on the key, which the fill hands back.
//
// Releasing inside the same script matters: a separate release would leave a window in which the
// value is present but the lease is still held, and a caller arriving in that window would be told
// to WAIT for a fill that has already finished.
//
// An empty token means the caller holds no lease — the negative-fill path and the CDC tailer both
// write without ever asking for one.
func (c *Client) FillWithLease(ctx context.Context, key string, e Entry, token string) (bool, error) {
	rdb, node, err := c.poolFor(key)
	if err != nil {
		return false, err
	}

	b := c.breakers.For(node)
	if !b.Allow() {
		// Shedding a fill costs only the hit this entry would have served later. The value is
		// already on its way to the caller from the database.
		//
		// The lease is deliberately NOT released here: this caller never reached the node, so it
		// cannot know whether its lease still exists. The TTL is what cleans it up, which is the
		// case that expiry exists for.
		return false, nil
	}

	negative := "0"
	if e.Negative {
		negative = "1"
	}

	applied, err := fillCAS.Run(ctx, rdb, []string{key, leaseKey(key)},
		encodeVersion(e.RowVersion),
		encodeVersion(e.FillVersion),
		e.Payload,
		negative,
		c.ttl.Milliseconds(),
		e.TenantID,
		e.Status,
		token,
	).Int64()
	if err != nil {
		b.Failure()
		return false, fmt.Errorf("cache: fill %s: %w", key, err)
	}
	b.Success()
	return applied == 1, nil
}

// LeaseOutcome is what a GetOrLease call decided.
type LeaseOutcome int

const (
	// LeaseHit means the entry was present and is returned; no lease was taken.
	LeaseHit LeaseOutcome = iota
	// LeaseGranted means this caller owns the fill for this key. Nobody else will read the origin
	// for it until the fill completes or the lease expires.
	LeaseGranted
	// LeaseWait means another caller is already filling. The right response is to wait briefly and
	// look again — and, if that does not resolve, to read the origin directly rather than block.
	LeaseWait
)

func (o LeaseOutcome) String() string {
	switch o {
	case LeaseHit:
		return "hit"
	case LeaseGranted:
		return "granted"
	case LeaseWait:
		return "wait"
	default:
		return "unknown"
	}
}

// LeaseResult is the answer to GetOrLease.
type LeaseResult struct {
	Outcome LeaseOutcome

	// Entry is set only on LeaseHit.
	Entry Entry

	// Token is set only on LeaseGranted, and must be handed back to FillWithLease.
	Token string
}

// GetOrLease reads an entry, or takes the exclusive right to fill it.
//
// One round trip decides both, and that is the mechanism rather than an optimisation. Reading and
// then competing for a lease leaves a window in which every caller has already seen a miss and
// decided to go to the database; under a stampede that window IS the stampede.
//
// A shed read returns LeaseWait, not LeaseGranted. Telling a caller it owns a fill when the breaker
// never let the request reach the node would hand out an admission right that no node has recorded
// — and every shed caller would receive one, which is the opposite of bounding origin load. Wait
// degrades correctly: the caller retries briefly and then reads the origin itself.
func (c *Client) GetOrLease(ctx context.Context, key string) (LeaseResult, error) {
	rdb, node, err := c.poolFor(key)
	if err != nil {
		return LeaseResult{}, err
	}

	b := c.breakers.For(node)
	if !b.Allow() {
		return LeaseResult{Outcome: LeaseWait}, nil
	}

	token, err := newLeaseToken()
	if err != nil {
		return LeaseResult{}, err
	}

	res, err := readLease.Run(ctx, rdb, []string{key, leaseKey(key)},
		token, c.leaseTTL.Milliseconds(),
	).Slice()
	if err != nil {
		b.Failure()
		return LeaseResult{}, fmt.Errorf("cache: get-or-lease %s: %w", key, err)
	}
	b.Success()

	if len(res) == 0 {
		return LeaseResult{}, fmt.Errorf("cache: get-or-lease %s: %w: empty reply", key, ErrCorruptEntry)
	}
	tag, ok := res[0].(int64)
	if !ok {
		return LeaseResult{}, fmt.Errorf("cache: get-or-lease %s: %w: tag is %T", key, ErrCorruptEntry, res[0])
	}

	switch tag {
	case 1:
		if len(res) != 7 {
			return LeaseResult{}, fmt.Errorf("cache: get-or-lease %s: %w: %d fields", key, ErrCorruptEntry, len(res))
		}
		entry, err := entryFromLua(res[1:])
		if err != nil {
			return LeaseResult{}, fmt.Errorf("cache: get-or-lease %s: %w", key, err)
		}
		return LeaseResult{Outcome: LeaseHit, Entry: entry}, nil
	case 2:
		return LeaseResult{Outcome: LeaseGranted, Token: token}, nil
	case 3:
		return LeaseResult{Outcome: LeaseWait}, nil
	default:
		return LeaseResult{}, fmt.Errorf("cache: get-or-lease %s: %w: tag %d", key, ErrCorruptEntry, tag)
	}
}

// leaseKey is the lease companion to an entry key.
//
// A separate key rather than a field on the entry hash, because a lease must be takeable on a key
// that has no entry at all — which is the only case that matters.
func leaseKey(key string) string { return key + "\x00lease" }

// newLeaseToken returns an unguessable token identifying one fill attempt.
//
// Random rather than sequential: the token is what stops a filler whose lease expired from
// releasing the lease its successor now holds, so two attempts on the same key must never collide.
func newLeaseToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("cache: generate lease token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// LeaseTTL returns the configured lease interval.
func (c *Client) LeaseTTL() time.Duration { return c.leaseTTL }

// Tombstone marks an entry invalidated at the given version, reporting whether it was applied.
//
// This replaces deletion on the write path. A plain delete loses the delete-versus-fill race: a
// read that started before the write can land afterwards and refill the old value, with nothing
// left to say it should not. The marker survives to reject exactly that fill.
func (c *Client) Tombstone(ctx context.Context, key string, version uint64) (bool, error) {
	rdb, node, err := c.poolFor(key)
	if err != nil {
		return false, err
	}

	// Deliberately NOT gated by the breaker, and the asymmetry is the point. Shedding a read costs
	// hit rate: the value comes from the database instead, and nobody is misinformed. Shedding an
	// invalidation costs correctness: the stale entry survives and the cache goes on serving a value
	// the database has already changed, with no record that it was told otherwise.
	//
	// So a tombstone is always attempted, and a tombstone that cannot be applied is returned as an
	// error rather than folded into a miss. The caller has to know its invalidation did not land —
	// that is what makes the CDC backstop's job well-defined instead of a guess.
	b := c.breakers.For(node)
	applied, err := tombstoneCAS.Run(ctx, rdb, []string{key},
		encodeVersion(version),
		c.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		b.Failure()
		return false, fmt.Errorf("cache: tombstone %s: %w", key, err)
	}
	b.Success()
	return applied == 1, nil
}

// entryFromLua decodes the four-element reply from read.lua.
func entryFromLua(res []any) (Entry, error) {
	rvRaw, ok := res[0].(string)
	if !ok {
		return Entry{}, fmt.Errorf("%w: row version is %T", ErrCorruptEntry, res[0])
	}
	rv, err := decodeVersion(rvRaw)
	if err != nil {
		return Entry{}, err
	}

	var fv uint64
	if fvRaw, ok := res[1].(string); ok {
		if fv, err = decodeVersion(fvRaw); err != nil {
			return Entry{}, err
		}
	}

	var payload []byte
	if p, ok := res[2].(string); ok {
		payload = []byte(p)
	}

	negative := false
	if n, ok := res[3].(string); ok {
		negative = n == "1"
	}

	// The row fields are optional on read: an entry written by an older build carries neither, and
	// reading it as a zero-valued row is better than failing the request. It reads as a slightly
	// wrong record exactly once, until the TTL or the next write replaces it.
	tenantID, err := decodeTenantID(res[4])
	if err != nil {
		return Entry{}, err
	}
	status, err := decodeStatus(res[5])
	if err != nil {
		return Entry{}, err
	}

	return Entry{
		RowVersion:  rv,
		FillVersion: fv,
		TenantID:    tenantID,
		Status:      status,
		Payload:     payload,
		Negative:    negative,
	}, nil
}

// decodeTenantID and decodeStatus narrow the entry's row fields.
//
// The range check after parsing is redundant with the bit width handed to ParseUint, and it is kept
// because it makes the invariant local: a reader — and the overflow linter — can see that the
// conversion cannot wrap without having to reason about an argument three lines up.
func decodeTenantID(raw any) (uint32, error) {
	v, err := decodeSmall(raw, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: tenant id: %w", ErrCorruptEntry, err)
	}
	if v > math.MaxUint32 {
		return 0, fmt.Errorf("%w: tenant id %d does not fit in a uint32", ErrCorruptEntry, v)
	}
	return uint32(v), nil
}

func decodeStatus(raw any) (uint8, error) {
	v, err := decodeSmall(raw, 8)
	if err != nil {
		return 0, fmt.Errorf("%w: status: %w", ErrCorruptEntry, err)
	}
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("%w: status %d does not fit in a uint8", ErrCorruptEntry, v)
	}
	return uint8(v), nil
}

// decodeSmall parses one of the entry's narrow numeric fields.
//
// A missing field decodes as zero rather than as an error, so an entry written before these fields
// existed still reads. A field that is PRESENT but unparseable is an error, because that means
// something other than Cachet is writing to these keys — which is worth surfacing rather than
// rounding to zero.
func decodeSmall(raw any, bits int) (uint64, error) {
	s, ok := raw.(string)
	if !ok || s == "" {
		return 0, nil
	}
	v, err := strconv.ParseUint(s, 10, bits)
	if err != nil {
		return 0, fmt.Errorf("bad value %q", s)
	}
	return v, nil
}

// Flush removes every entry.
//
// It exists for tests and for operator recovery, never for the request path: dropping the whole
// cache to fix one key is how a stale-data incident becomes an availability incident.
func (c *Client) Flush(ctx context.Context) error {
	// Every node, not just the first. A Flush that cleared one node would leave an operator
	// believing the cache was empty while the rest of it kept serving entries.
	for _, addr := range c.router.Nodes() {
		if err := c.pools[addr].FlushDB(ctx).Err(); err != nil {
			return fmt.Errorf("cache: flush %s: %w", addr, err)
		}
	}
	return nil
}

// TTL returns the configured entry lifetime.
func (c *Client) TTL() time.Duration { return c.ttl }

// RemainingTTL reports how much longer an entry will live, and whether it is there at all.
//
// It exists for operator tooling rather than the request path: "when does this expire?" is the
// second question anyone asks about a cached entry, right after "is it there?", and answering it by
// reading the configured TTL would be a guess rather than a measurement.
//
// Deliberately not gated by the breaker. This is a human asking a direct question about one key,
// not traffic to be shed, and an answer of "probably" would be useless during an incident.
func (c *Client) RemainingTTL(ctx context.Context, key string) (time.Duration, bool, error) {
	rdb, _, err := c.poolFor(key)
	if err != nil {
		return 0, false, err
	}

	d, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		return 0, false, fmt.Errorf("cache: ttl %s: %w", key, err)
	}
	// Redis reports -2 for "no such key" and -1 for "no expiry". The second must never happen here:
	// every entry is written with a TTL, so an entry without one is a bug worth surfacing rather
	// than rendering as a blank column.
	switch d {
	case -2:
		return 0, false, nil
	case -1:
		return 0, true, fmt.Errorf("cache: entry %s has no expiry set", key)
	default:
		return d, true, nil
	}
}

// Close releases the connection pool.
func (c *Client) Close() error {
	// Every pool gets closed even if an earlier one fails, so one bad node cannot leak the
	// goroutines belonging to the others (CONTRIBUTING.md rule 2).
	var firstErr error
	for addr, rdb := range c.pools {
		if err := rdb.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("cache: close %s: %w", addr, err)
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return nil
}
