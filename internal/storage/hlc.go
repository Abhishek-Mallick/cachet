// Package storage owns Cachet's uncached data path: version stamping, shard routing, and the
// MySQL reads and writes underneath them. It knows nothing about caching — that separation is
// deliberate, because the baseline this project measures against is exactly this package with no
// cache in front of it.
package storage

import (
	"sync"
	"time"
)

// Version is a hybrid logical clock timestamp packed into a single uint64:
//
//	┌──────────────── 48 bits ────────────────┬──── 16 bits ────┐
//	│        physical time (milliseconds)      │  logical count  │
//	└──────────────────────────────────────────┴─────────────────┘
//
// The packing matters as much as the clock: every cache mutation is a compare-and-set against a
// Version (CONSISTENCY.md §2), and those comparisons happen inside Lua scripts where a plain
// integer compare is the only affordable operation.
//
// Versions are monotonic and comparable WITHIN a shard only. Each shard runs its own clock, so
// comparing Versions from different shards is meaningless and is a bug. See ADR 0003.
type Version uint64

const (
	logicalBits = 16
	logicalMask = 1<<logicalBits - 1

	// MaxPhysicalMillis is the largest representable physical component: 2^48-1 milliseconds,
	// which runs to the year 10889.
	MaxPhysicalMillis = int64(1)<<(64-logicalBits) - 1

	// MaxLogical is the largest logical counter within one millisecond. Exceeding it borrows a
	// millisecond from the future rather than wrapping.
	MaxLogical = uint16(logicalMask)
)

// NewVersion packs a physical millisecond timestamp and a logical counter into a Version.
//
// It panics if physicalMillis is negative or exceeds MaxPhysicalMillis. Both indicate a
// programming error rather than bad request data — no real clock produces either.
func NewVersion(physicalMillis int64, logical uint16) Version {
	if physicalMillis < 0 || physicalMillis > MaxPhysicalMillis {
		panic("storage: physical millis out of range for a Version")
	}
	return Version(uint64(physicalMillis)<<logicalBits | uint64(logical))
}

// PhysicalMillis returns the wall-clock component in Unix milliseconds.
func (v Version) PhysicalMillis() int64 { return int64(v >> logicalBits) }

// Logical returns the counter that orders writes within a single millisecond.
func (v Version) Logical() uint16 { return uint16(v & logicalMask) }

// Time returns the physical component as a time.Time. It is an approximation of when the write
// happened, accurate to the millisecond and subject to the clock skew bound — do not treat it as
// an authoritative timestamp.
func (v Version) Time() time.Time { return time.UnixMilli(v.PhysicalMillis()) }

// Clock issues monotonically increasing Versions for one shard.
//
// A Clock is safe for concurrent use. It is deliberately mutex-guarded rather than lock-free: Next
// is called once per write, not once per read, so it is not on the hot path, and a mutex keeps the
// overflow and regression rules readable enough to verify by eye.
type Clock struct {
	now func() time.Time

	mu   sync.Mutex
	last Version
}

// NewClock returns a Clock that reads wall time from now. Passing an injectable clock rather than
// calling time.Now directly is what makes the skew and regression behaviour testable, which is the
// only reason to trust it.
func NewClock(now func() time.Time) *Clock {
	if now == nil {
		now = time.Now
	}
	return &Clock{now: now}
}

// Next returns the next Version, strictly greater than every Version this Clock has previously
// issued or observed.
//
// Three cases, and each one is a named test:
//
//   - wall clock has advanced: adopt it, reset the logical counter
//   - wall clock is at or behind the last issued version: keep the last physical component and
//     increment the logical counter, so a clock that jumps backwards cannot invert versions
//   - the logical counter has saturated: borrow the next millisecond rather than wrapping
func (c *Clock) Next() Version {
	c.mu.Lock()
	defer c.mu.Unlock()

	nowMillis := c.now().UnixMilli()
	if nowMillis > c.last.PhysicalMillis() {
		c.last = NewVersion(nowMillis, 0)
		return c.last
	}
	return c.advanceLogicalLocked()
}

// Now returns the current clock reading without consuming a logical tick.
//
// This is the fill version (CONSISTENCY.md §1): "this cache entry reflects shard state as of Now".
// It is deliberately distinct from Next — a fill happens on every read miss, and reads outnumber
// writes by design, so burning one of the 65,536 per-millisecond logical slots on each of them
// would exhaust the counter for no benefit. Fill versions need to be ordered, not unique.
//
// Now never returns a value below a Version this Clock has already issued or observed, so a wall
// clock that jumps backwards cannot make this engine's own writes invisible to its own session
// watermark.
//
// It does advance the clock's floor, even though it consumes no tick. That is not an optimisation
// detail — it is required for correctness. Without it, a Now() and a subsequent Next() in the same
// millisecond both return (ms, 0): an entry filled BEFORE a write would carry a fill version equal
// to that write's version, satisfy the session watermark check, and be served. That is a
// read-own-writes violation, and TestNowDoesNotConsumeAVersion exists to catch it.
func (c *Clock) Now() Version {
	c.mu.Lock()
	defer c.mu.Unlock()

	if candidate := NewVersion(c.now().UnixMilli(), 0); candidate > c.last {
		c.last = candidate
	}
	return c.last
}

// Observe merges a Version produced elsewhere — typically the version already stored on a row this
// engine is about to update.
//
// This is what keeps per-shard monotonicity when more than one engine instance writes the same
// shard. Without it, two engines with skewed clocks could each stamp writes from their own private
// timeline, a later write could carry a lower Version than an earlier one, and a stale fill would
// win a compare-and-set. That is silent staleness — the exact failure Cachet exists to eliminate.
func (c *Clock) Observe(v Version) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v > c.last {
		c.last = v
	}
}

// advanceLogicalLocked bumps the logical counter, rolling into the next millisecond on saturation.
// The caller must hold c.mu.
func (c *Clock) advanceLogicalLocked() Version {
	if c.last.Logical() == MaxLogical {
		// Borrowing from the future keeps the sequence strictly increasing. It costs at most one
		// millisecond of clock drift per 65,536 writes in the same millisecond on one shard — far
		// above any plausible per-shard write rate.
		c.last = NewVersion(c.last.PhysicalMillis()+1, 0)
		return c.last
	}
	c.last = NewVersion(c.last.PhysicalMillis(), c.last.Logical()+1)
	return c.last
}
