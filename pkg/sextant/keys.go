package sextant

import (
	"math/rand/v2"
	"sync"
)

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
