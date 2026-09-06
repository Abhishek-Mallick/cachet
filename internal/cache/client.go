package cache

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
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
}

// Client is Cachet's cache-side data path.
type Client struct {
	rdb *redis.Client
	ttl time.Duration
}

// New connects to the cache and verifies it answers.
//
// Failing at boot beats discovering on the first user request that the cache was never reachable.
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

	rdb := redis.NewClient(&redis.Options{
		Addr:         opts.Addresses[0],
		DialTimeout:  2 * time.Second,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
		PoolSize:     64,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("cache: ping %s: %w", opts.Addresses[0], err)
	}

	return &Client{rdb: rdb, ttl: opts.TTL}, nil
}

// Get reads one entry.
//
// A miss is reported as hit=false with no error, because a miss is the normal path rather than a
// failure — returning an error would make every cold read look like a fault and drown the ones that
// matter. A tombstoned entry reads as a miss: the marker is invisible to readers and exists only to
// make a late fill lose its compare-and-set.
func (c *Client) Get(ctx context.Context, key string) (Entry, bool, error) {
	res, err := readScript.Run(ctx, c.rdb, []string{key}).Slice()
	switch {
	case errors.Is(err, redis.Nil):
		return Entry{}, false, nil
	case err != nil:
		return Entry{}, false, fmt.Errorf("cache: get %s: %w", key, err)
	}
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
	negative := "0"
	if e.Negative {
		negative = "1"
	}

	applied, err := fillCAS.Run(ctx, c.rdb, []string{key},
		encodeVersion(e.RowVersion),
		encodeVersion(e.FillVersion),
		e.Payload,
		negative,
		c.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("cache: fill %s: %w", key, err)
	}
	return applied == 1, nil
}

// Tombstone marks an entry invalidated at the given version, reporting whether it was applied.
//
// This replaces deletion on the write path. A plain delete loses the delete-versus-fill race: a
// read that started before the write can land afterwards and refill the old value, with nothing
// left to say it should not. The marker survives to reject exactly that fill.
func (c *Client) Tombstone(ctx context.Context, key string, version uint64) (bool, error) {
	applied, err := tombstoneCAS.Run(ctx, c.rdb, []string{key},
		encodeVersion(version),
		c.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("cache: tombstone %s: %w", key, err)
	}
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
	if err := c.rdb.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("cache: flush: %w", err)
	}
	return nil
}

// TTL returns the configured entry lifetime.
func (c *Client) TTL() time.Duration { return c.ttl }

// Close releases the connection pool.
func (c *Client) Close() error {
	if err := c.rdb.Close(); err != nil {
		return fmt.Errorf("cache: close: %w", err)
	}
	return nil
}
