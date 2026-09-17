package engine

import "time"

// WaitPolicy decides how long a caller waits for someone else's fill before giving up and reading
// the origin itself.
//
// The giving-up is the important half. A lease holder that dies is indistinguishable, from the
// outside, from one that is nearly finished: a waiter cannot tell them apart and must not try. So
// the schedule is bounded, every caller is guaranteed to be served, and the worst a dead holder can
// do is add the wait to one round of requests before they fall through to the database.
//
// That is the whole trade leases make. Waiting costs latency on a miss; not waiting costs the
// origin a stampede. The policy is what makes the first cost bounded and stated rather than
// open-ended.
type WaitPolicy struct {
	attempts int
	base     time.Duration
	max      time.Duration
}

// NewWaitPolicy builds a policy with exponential backoff, clamped at max.
//
// Non-positive values are normalised rather than rejected: the config validator refuses them at
// boot with a message naming the field, and a policy constructed directly by tooling should not be
// able to produce a negative sleep.
func NewWaitPolicy(attempts int, base, max time.Duration) WaitPolicy {
	if attempts < 0 {
		attempts = 0
	}
	if base < 0 {
		base = 0
	}
	if max < base {
		max = base
	}
	return WaitPolicy{attempts: attempts, base: base, max: max}
}

// MaxAttempts is how many times a caller re-checks the cache before reading the origin.
//
// Zero is meaningful: leases still bound who FILLS, but nobody ever waits. Origin load goes
// unbounded again and no request is ever delayed — a legitimate trade for a latency-critical
// deployment, and the configuration a benchmark uses to measure what the waiting actually buys.
func (p WaitPolicy) MaxAttempts() int { return p.attempts }

// Backoff is how long to sleep before re-checking, on the given zero-based attempt.
//
// It grows, because retrying at a fixed interval turns every waiter into a poller at the same
// frequency — which trades a stampede on the database for a smaller one on the cache node. It is
// capped, because a delay longer than the origin read it is avoiding is worse than not waiting.
func (p WaitPolicy) Backoff(attempt int) time.Duration {
	if attempt < 0 || p.base <= 0 {
		return 0
	}
	d := p.base
	for i := 0; i < attempt && d < p.max; i++ {
		d *= 2
	}
	if d > p.max {
		d = p.max
	}
	return d
}
