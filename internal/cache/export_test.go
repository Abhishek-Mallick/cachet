package cache

import (
	"context"
	"fmt"
)

// SetRawForTest writes bytes that bypass Encode, so a test can plant a corrupt entry.
//
// It lives in export_test.go so it is compiled only for tests and never reaches the public API — a
// production type must not carry a method whose only purpose is to let a test write invalid data.
func (c *Client) SetRawForTest(ctx context.Context, key string, raw []byte) error {
	if err := c.rdb.Set(ctx, key, raw, c.ttl).Err(); err != nil {
		return fmt.Errorf("cache: set raw %s: %w", key, err)
	}
	return nil
}
