package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
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

// Get reads one entry. A miss is reported as hit=false with no error, because a miss is the normal
// path rather than a failure — returning an error would make every cold read look like a fault and
// drown the ones that matter.
func (c *Client) Get(ctx context.Context, key string) (Entry, bool, error) {
	raw, err := c.rdb.Get(ctx, key).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		return Entry{}, false, nil
	case err != nil:
		return Entry{}, false, fmt.Errorf("cache: get %s: %w", key, err)
	}

	entry, err := Decode(raw)
	if err != nil {
		// Corruption is surfaced rather than folded into a miss: a systematic encoding bug would
		// otherwise present itself as a mysterious drop in hit rate that nobody could explain.
		return Entry{}, false, fmt.Errorf("cache: get %s: %w", key, err)
	}
	return entry, true, nil
}

// Set writes one entry with the configured TTL.
func (c *Client) Set(ctx context.Context, key string, e Entry) error {
	if err := c.rdb.Set(ctx, key, e.Encode(), c.ttl).Err(); err != nil {
		return fmt.Errorf("cache: set %s: %w", key, err)
	}
	return nil
}

// Delete removes one entry.
//
// Phase 1 has no invalidation, so this exists for operator use and for tests. Phase 2 replaces it
// on the write path with a versioned tombstone, because a plain delete loses the delete-versus-fill
// race that the compare-and-set invariant is designed to close (CONSISTENCY.md §2).
func (c *Client) Delete(ctx context.Context, key string) error {
	if err := c.rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("cache: delete %s: %w", key, err)
	}
	return nil
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
