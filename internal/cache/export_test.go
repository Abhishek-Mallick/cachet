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
	if err := c.rdb.HSet(ctx, key, "v", badVersion, "f", badVersion, "p", "", "n", "0").Err(); err != nil {
		return fmt.Errorf("cache: set raw %s: %w", key, err)
	}
	return nil
}
