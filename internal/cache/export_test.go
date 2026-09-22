package cache

import (
	"context"
	"fmt"
)

// SetRawForTest writes an entry with an unparseable version, so a test can plant corruption.
//
// It lives in export_test.go so it is compiled only for tests and never reaches the public API — a
// production type must not carry a method whose only purpose is to let a test write invalid data.
func (c *Client) SetRawForTest(ctx context.Context, key string, badVersion string) error {
	rdb, _, err := c.poolFor(key)
	if err != nil {
		return err
	}
	// The fingerprint is this client's own, so the entry passes the Lua's shape check and reaches
	// the decoder. Without it the entry would read as a miss — which is correct behaviour for a
	// foreign shape, and would mean this helper planted something the decoder never sees.
	if err := rdb.HSet(ctx, key,
		"v", badVersion, "f", badVersion, "r", "", "n", "0", "h", c.fingerprint).Err(); err != nil {
		return fmt.Errorf("cache: set raw %s: %w", key, err)
	}
	return nil
}

// LeaseHeldForTest reports whether a lease is currently held for a key.
//
// It lives in export_test.go because the lease key is an internal detail: a test that reconstructed
// it would silently stop testing anything the day the layout changed.
func (c *Client) LeaseHeldForTest(ctx context.Context, key string) (bool, error) {
	rdb, _, err := c.poolFor(key)
	if err != nil {
		return false, err
	}
	n, err := rdb.Exists(ctx, leaseKey(key)).Result()
	if err != nil {
		return false, fmt.Errorf("cache: lease held %s: %w", key, err)
	}
	return n == 1, nil
}
