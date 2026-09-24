// Package sextant wires the published verifier to THIS deployment.
//
// The rule that decides what a violation is lives in pkg/sextant. What lives here is the pair of
// adapters that read Cachet's own cache client and its sharded storage — the only parts that know
// which system is being checked.
package sextant

import (
	"context"
	"errors"
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// The adapters that point the verifier at THIS deployment — Cachet's own cache client and its
// sharded storage. They are deliberately thin: everything that decides whether something is a
// violation lives in pkg/sextant, so there is exactly one place
// where the rule can be wrong.

// CacheAdapter reads entries from a live cache without being able to change them.
//
// It wraps the client rather than embedding it, which is the whole point: the verifier gets Peek
// and nothing else. A component that could fill or tombstone the cache it is checking would be able
// to influence its own findings, and no number it produced afterwards would be worth anything.
type CacheAdapter struct{ client *cache.Client }

// NewCacheAdapter wraps a cache client for read-only verification.
func NewCacheAdapter(c *cache.Client) *CacheAdapter { return &CacheAdapter{client: c} }

// Peek returns the fill version of a cached entry.
//
// The FILL version, not the row version, because the question is "how stale is the database
// snapshot behind this entry" — which is the question the guarantee is written in terms of
// (CONSISTENCY.md §1). Comparing row versions would ask a different question and answer it
// confidently.
func (a *CacheAdapter) Peek(ctx context.Context, key string) (uint64, bool, error) {
	entry, hit, err := a.client.Get(ctx, key)
	if err != nil {
		return 0, false, err
	}
	if !hit {
		return 0, false, nil
	}
	return entry.FillVersion, true, nil
}

// OriginAdapter reads row versions from the sharded database.
type OriginAdapter struct {
	router *storage.Router
	shards map[storage.ShardID]*storage.Shard
}

// NewOriginAdapter wraps the storage layer for verification.
func NewOriginAdapter(router *storage.Router, shards map[storage.ShardID]*storage.Shard) *OriginAdapter {
	return &OriginAdapter{router: router, shards: shards}
}

// Version returns a row's current version.
//
// Read through the shard's declared table rather than a statement written here, so the verifier
// reads the same columns the engine caches. A second way to read the origin is a second thing that
// can be wrong about what the origin holds, which is the one thing a verifier must not be.
func (a *OriginAdapter) Version(ctx context.Context, key string) (uint64, bool, error) {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		return 0, false, fmt.Errorf("sextant: parse key %q: %w", key, err)
	}
	shardID, err := a.router.ShardFor(key)
	if err != nil {
		return 0, false, fmt.Errorf("sextant: route %s: %w", key, err)
	}
	shard, ok := a.shards[shardID]
	if !ok {
		return 0, false, fmt.Errorf("sextant: no open shard %s", shardID)
	}

	row, _, err := shard.GetRow(ctx, parsed)
	if errors.Is(err, storage.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("sextant: read %s: %w", key, err)
	}

	t, err := shard.Table(parsed.Table)
	if err != nil {
		return 0, false, fmt.Errorf("sextant: %w", err)
	}
	version, err := row[t.Descriptor().VersionColumn.Index].Uint64()
	if err != nil {
		return 0, false, fmt.Errorf("sextant: read %s: %w", key, err)
	}
	return version, true, nil
}

// Shard names the shard a key belongs to.
func (a *OriginAdapter) Shard(key string) (string, error) {
	id, err := a.router.ShardFor(key)
	if err != nil {
		return "", fmt.Errorf("sextant: route %s: %w", key, err)
	}
	return string(id), nil
}
