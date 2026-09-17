package engine_test

import (
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/engine"
)

// The wait protocol is where a lease turns from a primitive into bounded origin load — and it is
// also the place a lease can do real harm. A caller told to WAIT is a caller not being served, so
// every property here is about the ceiling on that harm rather than about the happy path.

func TestWaitScheduleIsBounded(t *testing.T) {
	t.Parallel()

	// The single most important property. A lease holder that dies is invisible to everyone waiting
	// on it: they cannot tell "filling, nearly done" from "died three seconds ago". So waiting must
	// end on its own and the caller must go to the database, always. Blocking on a holder that will
	// never return is a self-inflicted outage on the hottest key in the system.
	p := engine.NewWaitPolicy(5, 10*time.Millisecond, 100*time.Millisecond)

	total := time.Duration(0)
	for i := 0; i < p.MaxAttempts(); i++ {
		total += p.Backoff(i)
	}
	if total > 500*time.Millisecond {
		t.Errorf("a full wait schedule takes %s; a caller can be held for longer than a slow origin read", total)
	}
	if p.MaxAttempts() != 5 {
		t.Errorf("MaxAttempts() = %d, want 5", p.MaxAttempts())
	}
}

func TestBackoffGrows(t *testing.T) {
	t.Parallel()

	// Retrying at a fixed interval turns every waiter into a poller at the same frequency, which
	// replaces one stampede on the database with a smaller one on the cache node. Growing the gap
	// spreads them out.
	p := engine.NewWaitPolicy(6, 10*time.Millisecond, time.Second)

	prev := time.Duration(0)
	for i := 0; i < p.MaxAttempts(); i++ {
		got := p.Backoff(i)
		if got < prev {
			t.Errorf("backoff shrank at attempt %d: %s after %s", i, got, prev)
		}
		prev = got
	}
}

func TestBackoffIsCapped(t *testing.T) {
	t.Parallel()

	// Unbounded growth would eventually make a late waiter's delay longer than the database read it
	// is waiting to avoid, which is the point at which waiting stops being worth anything.
	p := engine.NewWaitPolicy(20, 10*time.Millisecond, 50*time.Millisecond)

	for i := 0; i < p.MaxAttempts(); i++ {
		if got := p.Backoff(i); got > 50*time.Millisecond {
			t.Fatalf("backoff at attempt %d is %s, over the %s cap", i, got, 50*time.Millisecond)
		}
	}
}

func TestZeroAttemptsMeansNeverWait(t *testing.T) {
	t.Parallel()

	// Turning the wait off must be expressible. With leases enabled but attempts at zero, a caller
	// told to WAIT goes straight to the database — origin load is unbounded again, but nobody is
	// ever delayed. That is a legitimate trade for a latency-critical deployment, and it is also
	// the configuration a benchmark uses to measure what leases are actually buying.
	p := engine.NewWaitPolicy(0, 10*time.Millisecond, time.Second)
	if p.MaxAttempts() != 0 {
		t.Errorf("MaxAttempts() = %d, want 0", p.MaxAttempts())
	}
}

func TestNegativeOrZeroValuesAreNormalised(t *testing.T) {
	t.Parallel()

	// A misconfiguration must not produce a negative sleep or an unbounded one. Config validation
	// catches these at boot; this is the second line, because the policy is also constructed
	// directly in tests and tooling.
	p := engine.NewWaitPolicy(-3, -time.Second, -time.Second)
	if p.MaxAttempts() < 0 {
		t.Errorf("MaxAttempts() = %d, want it clamped to zero or more", p.MaxAttempts())
	}
	for i := 0; i < 3; i++ {
		if got := p.Backoff(i); got < 0 {
			t.Errorf("Backoff(%d) = %s, want a non-negative duration", i, got)
		}
	}
}
