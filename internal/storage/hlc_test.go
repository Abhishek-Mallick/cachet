package storage_test

import (
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// fakeClock lets the tests drive wall time directly. HLC's entire purpose is to behave correctly
// when the wall clock misbehaves, so the wall clock has to be an input, not an ambient fact.
type fakeClock struct {
	mu     sync.Mutex
	millis int64
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return time.UnixMilli(f.millis)
}

func (f *fakeClock) set(ms int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.millis = ms
}

func TestVersionPacksPhysicalAndLogical(t *testing.T) {
	t.Parallel()

	v := storage.NewVersion(1_700_000_000_123, 42)

	if got := v.PhysicalMillis(); got != 1_700_000_000_123 {
		t.Errorf("PhysicalMillis() = %d, want 1700000000123", got)
	}
	if got := v.Logical(); got != 42 {
		t.Errorf("Logical() = %d, want 42", got)
	}
}

func TestVersionsOrderByPhysicalThenLogical(t *testing.T) {
	t.Parallel()

	ordered := []storage.Version{
		storage.NewVersion(100, 0),
		storage.NewVersion(100, 1),
		storage.NewVersion(100, 65535),
		storage.NewVersion(101, 0),
	}

	// The whole CAS invariant rests on versions being comparable as plain uint64s.
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1] >= ordered[i] {
			t.Errorf("version %d (%v) is not less than %d (%v)", i-1, ordered[i-1], i, ordered[i])
		}
	}
}

func TestNextAdvancesPhysicalWhenClockMovesForward(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 1000}
	c := storage.NewClock(clk.now)

	first := c.Next()
	clk.set(2000)
	second := c.Next()

	if second.PhysicalMillis() != 2000 {
		t.Errorf("PhysicalMillis() = %d, want 2000", second.PhysicalMillis())
	}
	if second.Logical() != 0 {
		t.Errorf("Logical() = %d, want 0 — a new millisecond resets the counter", second.Logical())
	}
	if first >= second {
		t.Errorf("second version %v is not greater than first %v", second, first)
	}
}

func TestNextIncrementsLogicalWithinTheSameMillisecond(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 1000}
	c := storage.NewClock(clk.now)

	a, b, d := c.Next(), c.Next(), c.Next()

	if a.PhysicalMillis() != 1000 || b.PhysicalMillis() != 1000 || d.PhysicalMillis() != 1000 {
		t.Fatalf("physical components drifted: %d %d %d", a.PhysicalMillis(), b.PhysicalMillis(), d.PhysicalMillis())
	}
	if a.Logical() != 0 || b.Logical() != 1 || d.Logical() != 2 {
		t.Errorf("logical components = %d %d %d, want 0 1 2", a.Logical(), b.Logical(), d.Logical())
	}
}

func TestNextStaysMonotonicWhenTheClockGoesBackwards(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 5000}
	c := storage.NewClock(clk.now)

	before := c.Next()
	clk.set(4000) // NTP correction, VM migration, or a badly behaved hypervisor.
	after := c.Next()

	// This is the whole reason for choosing HLC over the wall clock (ADR 0003). A version inversion
	// here would let a stale fill win a compare-and-set, which is silent staleness.
	if after <= before {
		t.Errorf("clock moved backwards and versions inverted: before=%v after=%v", before, after)
	}
	if after.PhysicalMillis() != 5000 {
		t.Errorf("PhysicalMillis() = %d, want 5000 — physical must not regress", after.PhysicalMillis())
	}
}

func TestNextBorrowsAMillisecondWhenTheLogicalCounterSaturates(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 7000}
	c := storage.NewClock(clk.now)

	// 65,536 versions exhausts the 16-bit logical counter within a single millisecond.
	var last storage.Version
	for i := 0; i < 65536; i++ {
		last = c.Next()
	}
	if last.PhysicalMillis() != 7000 || last.Logical() != 65535 {
		t.Fatalf("after 65536 calls: physical=%d logical=%d, want 7000/65535", last.PhysicalMillis(), last.Logical())
	}

	// The next one must roll into the following millisecond rather than wrapping the counter.
	// Wrapping would produce a LOWER version than the previous write.
	overflow := c.Next()
	if overflow <= last {
		t.Fatalf("logical counter wrapped: last=%v overflow=%v", last, overflow)
	}
	if overflow.PhysicalMillis() != 7001 || overflow.Logical() != 0 {
		t.Errorf("overflow = physical %d logical %d, want 7001/0", overflow.PhysicalMillis(), overflow.Logical())
	}
}

func TestObserveAdoptsAHigherRemoteVersion(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 1000}
	c := storage.NewClock(clk.now)

	// Another engine instance, whose clock runs ahead, already stamped this row.
	remote := storage.NewVersion(9000, 3)
	c.Observe(remote)

	next := c.Next()

	// Without this, two engines writing the same shard could interleave version inversions and the
	// per-shard monotonicity the CAS invariant depends on would not hold. CONSISTENCY.md §2.
	if next <= remote {
		t.Errorf("Next() = %v after observing %v; must exceed it", next, remote)
	}
}

func TestObserveIgnoresAnOlderRemoteVersion(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 5000}
	c := storage.NewClock(clk.now)

	current := c.Next()
	c.Observe(storage.NewVersion(10, 0)) // an ancient row
	next := c.Next()

	if next.PhysicalMillis() != 5000 {
		t.Errorf("PhysicalMillis() = %d, want 5000 — an old observation must not drag the clock back", next.PhysicalMillis())
	}
	if next <= current {
		t.Errorf("Next() = %v is not greater than %v", next, current)
	}
}

func TestNextIsMonotonicUnderConcurrentCallers(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 1000}
	c := storage.NewClock(clk.now)

	const goroutines, perGoroutine = 16, 500

	var wg sync.WaitGroup
	results := make([][]storage.Version, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([]storage.Version, perGoroutine)
			for i := range out {
				out[i] = c.Next()
			}
			results[g] = out
		}(g)
	}
	wg.Wait()

	seen := make(map[storage.Version]struct{}, goroutines*perGoroutine)
	for _, batch := range results {
		for _, v := range batch {
			if _, dup := seen[v]; dup {
				t.Fatalf("duplicate version issued: %v", v)
			}
			seen[v] = struct{}{}
		}
	}
	if len(seen) != goroutines*perGoroutine {
		t.Errorf("issued %d distinct versions, want %d", len(seen), goroutines*perGoroutine)
	}
}

func TestNowDoesNotConsumeAVersion(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 3000}
	c := storage.NewClock(clk.now)

	// Now() is read on every cache fill to stamp the entry's fill version, so it must not burn
	// logical counter space the way Next() does — reads outnumber writes by design.
	a := c.Now()
	b := c.Now()
	if a != b {
		t.Errorf("two consecutive Now() calls returned %v and %v; Now must not consume", a, b)
	}

	if next := c.Next(); next <= a {
		t.Errorf("Next() = %v after Now() = %v; Next must still advance past it", next, a)
	}
}

func TestNowNeverPrecedesAnIssuedVersion(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{millis: 3000}
	c := storage.NewClock(clk.now)

	issued := c.Next()
	clk.set(2500) // the wall clock jumps backwards after the write

	// A fill version below an already-issued write version would make this engine's own writes look
	// invisible to its own session watermark. Now() must be conservative, never regressive.
	if got := c.Now(); got < issued {
		t.Errorf("Now() = %v is behind the already-issued version %v", got, issued)
	}
}
