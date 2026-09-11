package cache

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
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
)

// go-redis's Script wrapper tries EVALSHA first and falls back to EVAL on a NOSCRIPT reply, so a
// cache server that restarts and loses its script cache recovers without a round trip per call.
var (
	fillCAS      = redis.NewScript(fillCASSource)
	tombstoneCAS = redis.NewScript(tombstoneCASSource)
	readScript   = redis.NewScript(readSource)
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
}

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

	c := &Client{
		router:   router,
		pools:    make(map[string]*redis.Client, len(router.Nodes())),
		breakers: breakers,
		ttl:      opts.TTL,
	}
	for _, addr := range router.Nodes() {
		rdb := redis.NewClient(&redis.Options{
			Addr:         addr,
			DialTimeout:  2 * time.Second,
			ReadTimeout:  timeout,
			WriteTimeout: timeout,
			PoolSize:     64,
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			_ = rdb.Close()
			_ = c.Close()
			return nil, fmt.Errorf("cache: ping %s: %w", addr, err)
		}
		c.pools[addr] = rdb
	}
	return c, nil
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
	if len(res) != 4 {
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
	rdb, node, err := c.poolFor(key)
	if err != nil {
		return false, err
	}

	b := c.breakers.For(node)
	if !b.Allow() {
		// Shedding a fill costs only the hit this entry would have served later. The value is
		// already on its way to the caller from the database.
		return false, nil
	}

	negative := "0"
	if e.Negative {
		negative = "1"
	}

	applied, err := fillCAS.Run(ctx, rdb, []string{key},
		encodeVersion(e.RowVersion),
		encodeVersion(e.FillVersion),
		e.Payload,
		negative,
		c.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		b.Failure()
		return false, fmt.Errorf("cache: fill %s: %w", key, err)
	}
	b.Success()
	return applied == 1, nil
}

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

	return Entry{RowVersion: rv, FillVersion: fv, Payload: payload, Negative: negative}, nil
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
