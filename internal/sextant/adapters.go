package sextant

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// The adapters that point the verifier at a real deployment. They are deliberately thin: everything
// that decides whether something is a violation lives in verify.go, so there is exactly one place
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
func (a *OriginAdapter) Version(ctx context.Context, key string) (uint64, bool, error) {
	id, err := parseKeyID(key)
	if err != nil {
		return 0, false, err
	}
	shardID, err := a.router.ShardFor(key)
	if err != nil {
		return 0, false, fmt.Errorf("sextant: route %s: %w", key, err)
	}
	shard, ok := a.shards[shardID]
	if !ok {
		return 0, false, fmt.Errorf("sextant: no open shard %s", shardID)
	}

	rec, _, err := shard.Get(ctx, id)
	if errors.Is(err, storage.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("sextant: read %s: %w", key, err)
	}
	return uint64(rec.Version), true, nil
}

// Shard names the shard a key belongs to.
func (a *OriginAdapter) Shard(key string) (string, error) {
	id, err := a.router.ShardFor(key)
	if err != nil {
		return "", fmt.Errorf("sextant: route %s: %w", key, err)
	}
	return string(id), nil
}

// parseKeyID extracts the numeric id from "entities:<id>".
func parseKeyID(key string) (uint64, error) {
	var id uint64
	if _, err := fmt.Sscanf(key, "entities:%d", &id); err != nil {
		return 0, fmt.Errorf("sextant: parse key %q: %w", key, err)
	}
	return id, nil
}

// RecentKeys is a bounded, recency-weighted source of keys to check.
//
// Weighted toward recently invalidated keys rather than sampled uniformly (build plan §10.5),
// because a key nobody has written cannot be stale. Uniform sampling would spend most of a fixed
// budget confirming that untouched rows are still correct, which is true and useless.
//
// The bound matters as much as the weighting: this runs beside a live system, so it must not grow
// with the workload.
type RecentKeys struct {
	capacity int

	mu    sync.Mutex
	keys  []string
	index map[string]int
	rng   *rand.Rand
}

// NewRecentKeys builds a key source holding at most capacity keys.
func NewRecentKeys(capacity int) *RecentKeys {
	if capacity <= 0 {
		capacity = 10_000
	}
	return &RecentKeys{
		capacity: capacity,
		index:    make(map[string]int, capacity),
		rng:      rand.New(rand.NewPCG(1, 2)),
	}
}

// Touch records that a key was written, making it a candidate for checking.
func (r *RecentKeys) Touch(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.index[key]; ok {
		return
	}
	if len(r.keys) >= r.capacity {
		// Drop the oldest. A key written long enough ago has either converged or been reported
		// already, so it is the cheapest one to stop watching.
		oldest := r.keys[0]
		r.keys = r.keys[1:]
		delete(r.index, oldest)
		for i, k := range r.keys {
			r.index[k] = i
		}
	}
	r.index[key] = len(r.keys)
	r.keys = append(r.keys, key)
}

// Next returns a key to check, chosen at random from those recently written.
//
// Random rather than round-robin so that a key which is stale only intermittently is still
// eventually sampled: a fixed order would synchronise with any periodic workload and could miss the
// same key every round forever.
func (r *RecentKeys) Next() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.keys) == 0 {
		return "", false
	}
	return r.keys[r.rng.IntN(len(r.keys))], true
}

// Len is how many keys are currently candidates.
func (r *RecentKeys) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}
