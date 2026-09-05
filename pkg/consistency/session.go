package consistency

import "sync"

// DefaultMaxSessionShards caps how many per-shard watermarks a session token carries.
//
// The cap exists because the token travels in request metadata on every call: an unbounded token
// grows with the number of shards a long-lived session has ever touched, and a header that grows
// without limit is a slow-motion outage.
const DefaultMaxSessionShards = 64

// Token holds a client's causal position: the highest version it has written or observed, per
// shard.
//
// It is a sparse map rather than a scalar because every shard runs its own hybrid logical clock, so
// versions from different shards are incomparable and comparing them is a bug (ADR 0003).
//
// The session is held by the CLIENT, not the server. That is what lets the read-own-writes
// guarantee survive a reconnect or an engine failover — engines are stateless with respect to
// sessions, so there is nothing on the server side to lose. The corollary is stated plainly in
// CONSISTENCY.md §4: lose the token and you have a new session, which is correct behaviour rather
// than a bug, because a process that has forgotten it wrote something has no writes to read.
//
// A Token is safe for concurrent use: the SDK shares one across an application's goroutines,
// which is the normal case rather than an edge case.
type Token struct {
	max int

	mu      sync.RWMutex
	seq     uint64
	entries map[string]watermark
	// evicted remembers shards whose watermark was dropped to stay under the cap. It is the
	// difference between "this session never wrote there" and "this session wrote there and we can
	// no longer prove it" — only the second is a degraded guarantee.
	evicted map[string]struct{}
}

type watermark struct {
	version uint64
	// used is a monotonic counter, not a timestamp: eviction must be deterministic and must not
	// depend on the clock, since the clock is exactly the thing this system does not trust.
	used uint64
}

// NewToken returns an empty session token carrying at most maxShards watermarks.
//
// A non-positive cap falls back to the default rather than being honoured: a cap of zero would
// evict every watermark immediately and silently disable read-own-writes, which is a worse outcome
// than ignoring a nonsensical argument.
func NewToken(maxShards int) *Token {
	if maxShards <= 0 {
		maxShards = DefaultMaxSessionShards
	}
	return &Token{
		max:     maxShards,
		entries: make(map[string]watermark, maxShards),
		evicted: make(map[string]struct{}),
	}
}

// Advance records that this session has written or observed version v on shard.
//
// The watermark only ever moves forward. A watermark that could regress would let a session read a
// value older than one it has already seen, which breaks monotonic reads.
func (s *Token) Advance(shard string, v uint64) {
	if shard == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceLocked(shard, v)
}

func (s *Token) advanceLocked(shard string, v uint64) {
	s.seq++

	if cur, ok := s.entries[shard]; ok {
		if v > cur.version {
			cur.version = v
		}
		cur.used = s.seq
		s.entries[shard] = cur
		return
	}

	if len(s.entries) >= s.max {
		s.evictOldestLocked()
	}
	s.entries[shard] = watermark{version: v, used: s.seq}
	delete(s.evicted, shard)
}

// evictOldestLocked drops the least recently used watermark and remembers that it was dropped.
// The caller must hold s.mu.
func (s *Token) evictOldestLocked() {
	var (
		oldestShard string
		oldestUsed  uint64
		found       bool
	)
	for shard, w := range s.entries {
		if !found || w.used < oldestUsed {
			oldestShard, oldestUsed, found = shard, w.used, true
		}
	}
	if !found {
		return
	}
	delete(s.entries, oldestShard)
	s.evicted[oldestShard] = struct{}{}
}

// Watermark returns the session's position on shard. known is false when this session holds no
// watermark for it — either because it never touched that shard, or because the watermark was
// evicted. Degraded distinguishes the two.
func (s *Token) Watermark(shard string) (version uint64, known bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	w, ok := s.entries[shard]
	return w.version, ok
}

// Degraded reports whether this session once held a watermark for shard and no longer does.
//
// A read against such a shard cannot honour SESSION and falls back to BOUNDED, and the response
// must say so. An untouched shard is not degraded: a session that has written nothing there has no
// writes to read.
func (s *Token) Degraded(shard string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, evicted := s.evicted[shard]
	return evicted
}

// Merge folds another session's watermarks into this one, taking the higher version per shard.
//
// This is causal propagation across a service boundary: if A writes and C reads, C needs A's
// watermark, and the merge must never lose the higher of the two.
func (s *Token) Merge(other map[string]uint64) {
	if len(other) == 0 {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for shard, v := range other {
		if shard == "" {
			continue
		}
		s.advanceLocked(shard, v)
	}
}

// Watermarks returns a copy of the session's positions, suitable for putting on the wire.
func (s *Token) Watermarks() map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]uint64, len(s.entries))
	for shard, w := range s.entries {
		out[shard] = w.version
	}
	return out
}
