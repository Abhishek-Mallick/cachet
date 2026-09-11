// Package breaker implements Cachet's proportional circuit breaker.
//
// The usual circuit breaker is a latch: once the failure rate crosses a threshold it trips open and
// sheds ALL traffic to the unhealthy dependency. For a cache node that behaviour is actively
// harmful. A node failing 30% of the time is still answering 70% of its reads; tripping it fully
// open throws that 70% away and sends every key it owns to the database at once. A partial cache
// degradation becomes a total one, and the load step is delivered to the origin as a cliff rather
// than a slope — at the exact moment the system is least able to absorb it.
//
// So this breaker sheds a FRACTION of traffic, scaled to the failure rate it actually observes
// (product spec §6, Tier 0). Two properties matter and both are asserted by tests:
//
//   - Shedding is proportional and monotonic. 40% failures sheds roughly 40% of traffic, not 100%.
//   - Shedding is capped below 1, so a probe trickle always reaches even a node that is failing
//     every request. Without it the breaker could never observe recovery and would stay open until
//     the process restarted — a self-inflicted outage outliving the one that caused it.
//
// Shedding a cache request is not an error. It degrades to a miss, and the read proceeds to the
// database. The breaker trades hit rate for latency: it stops paying a timeout to a node that is
// probably not going to answer.
package breaker

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Options configures a Breaker.
type Options struct {
	// Window is how far back the breaker looks when judging a node's health.
	Window time.Duration

	// Buckets is how finely the window is subdivided. More buckets means failures age out more
	// smoothly; too few and the whole window drops at once, making shedding oscillate.
	Buckets int

	// MinRequests is the evidence threshold. Below it the breaker sheds nothing, regardless of the
	// failure rate: two failures out of two is a 100% failure rate and means nothing at all.
	MinRequests int

	// FailureFloor is the failure rate tolerated without shedding. Every healthy node has a nonzero
	// error rate — timeouts, restarts, a rebalance — and treating that as unhealthy would shed
	// traffic permanently for no gain.
	FailureFloor float64

	// MaxShed caps the shed fraction. It must be below 1: the remainder is the probe traffic that
	// makes recovery self-detecting.
	MaxShed float64

	// Now and Rand are injectable so tests can age the window and assert exact shed fractions
	// instead of sleeping and hoping. Both default to the real thing.
	Now  func() time.Time
	Rand func() float64
}

// Stats is a breaker's view of a node over the current window.
//
// It exists so an operator can be told WHY a node is being shed. "30 of the last 100 reads failed"
// is an explanation someone can act on; a bare probability is a number nobody can argue with.
type Stats struct {
	Successes   int
	Failures    int
	Total       int
	FailureRate float64
	ShedRate    float64
}

// Breaker tracks one node's health and decides what fraction of its traffic to shed.
//
// Safe for concurrent use.
type Breaker struct {
	opts     Options
	bucketed time.Duration

	mu      sync.Mutex
	buckets []bucket
	// start is the beginning of the bucket at index head.
	head      int
	headStart time.Time
}

type bucket struct {
	successes int
	failures  int
}

// New builds a Breaker.
func New(opts Options) (*Breaker, error) {
	if opts.Window <= 0 {
		return nil, fmt.Errorf("breaker: window must be positive, got %s", opts.Window)
	}
	if opts.Buckets <= 0 {
		return nil, fmt.Errorf("breaker: buckets must be positive, got %d", opts.Buckets)
	}
	if opts.MinRequests < 0 {
		return nil, fmt.Errorf("breaker: min_requests must not be negative, got %d", opts.MinRequests)
	}
	if opts.FailureFloor < 0 || opts.FailureFloor >= 1 {
		return nil, fmt.Errorf("breaker: failure_floor must be in [0,1), got %v", opts.FailureFloor)
	}
	if opts.MaxShed < 0 {
		return nil, fmt.Errorf("breaker: max_shed must not be negative, got %v", opts.MaxShed)
	}
	if opts.MaxShed >= 1 {
		// Shedding everything means never calling the node again, which means never learning it
		// recovered. The breaker would latch open until the process restarted.
		return nil, errors.New("breaker: max_shed must be below 1 so probe traffic can detect recovery")
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Rand == nil {
		opts.Rand = rand.Float64
	}

	return &Breaker{
		opts:      opts,
		bucketed:  opts.Window / time.Duration(opts.Buckets),
		buckets:   make([]bucket, opts.Buckets),
		headStart: opts.Now(),
	}, nil
}

// Success records a call that worked.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollLocked()
	b.buckets[b.head].successes++
}

// Failure records a call that did not.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rollLocked()
	b.buckets[b.head].failures++
}

// Allow reports whether this request should be sent to the node.
//
// False means shed: skip the cache and go to the database. It is a degraded hit, not an error.
func (b *Breaker) Allow() bool {
	p := b.ShedProbability()
	if p <= 0 {
		return true
	}
	return b.opts.Rand() >= p
}

// ShedProbability is the fraction of traffic currently being shed.
func (b *Breaker) ShedProbability() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.shedLocked()
}

// Stats returns what the breaker has observed over the current window.
func (b *Breaker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()

	successes, failures := b.countLocked()
	total := successes + failures

	s := Stats{Successes: successes, Failures: failures, Total: total, ShedRate: b.shedLocked()}
	if total > 0 {
		s.FailureRate = float64(failures) / float64(total)
	}
	return s
}

// shedLocked computes the shed fraction. Callers must hold b.mu.
func (b *Breaker) shedLocked() float64 {
	b.rollLocked()

	successes, failures := b.countLocked()
	total := successes + failures
	if total < b.opts.MinRequests {
		return 0
	}

	rate := float64(failures) / float64(total)
	if rate <= b.opts.FailureFloor {
		return 0
	}

	// Rescale the interval above the floor onto [0, MaxShed]: a node at the floor sheds nothing,
	// a node failing every request sheds MaxShed and keeps the remainder as probes. Linear because
	// the claim being made is "proportional" — a curve would be a tuning choice nobody could read
	// off the graph, and this number appears in operator-facing output.
	shed := (rate - b.opts.FailureFloor) / (1 - b.opts.FailureFloor) * b.opts.MaxShed
	if shed > b.opts.MaxShed {
		shed = b.opts.MaxShed
	}
	return shed
}

// countLocked sums the window. Callers must hold b.mu.
func (b *Breaker) countLocked() (successes, failures int) {
	for _, bk := range b.buckets {
		successes += bk.successes
		failures += bk.failures
	}
	return successes, failures
}

// rollLocked advances the ring to the current time, clearing buckets that have aged out of the
// window. Callers must hold b.mu.
func (b *Breaker) rollLocked() {
	elapsed := b.opts.Now().Sub(b.headStart)
	if elapsed < b.bucketed {
		return
	}

	steps := int(elapsed / b.bucketed)
	if steps >= len(b.buckets) {
		// The whole window has passed with no traffic. Everything the breaker knew is stale, and
		// continuing to shed on it would punish a node for an outage that ended long ago.
		for i := range b.buckets {
			b.buckets[i] = bucket{}
		}
		b.head = 0
		b.headStart = b.opts.Now()
		return
	}

	for i := 0; i < steps; i++ {
		b.head = (b.head + 1) % len(b.buckets)
		b.buckets[b.head] = bucket{}
	}
	b.headStart = b.headStart.Add(time.Duration(steps) * b.bucketed)
}

// Group holds one Breaker per node.
//
// Per-node is the whole point: one sick cache node must not cost the others their traffic. A single
// shared breaker would turn the loss of one node into degraded service across the entire ring,
// which is the opposite of what the independent cache ring is for.
type Group struct {
	opts Options

	mu       sync.Mutex
	breakers map[string]*Breaker
}

// NewGroup builds a Group whose breakers all share opts.
func NewGroup(opts Options) (*Group, error) {
	// Validate once, here, so a bad configuration fails at boot rather than on the first request to
	// whichever node happens to be touched first.
	if _, err := New(opts); err != nil {
		return nil, err
	}
	return &Group{opts: opts, breakers: make(map[string]*Breaker)}, nil
}

// For returns the breaker for a node, creating it on first use.
//
// The same node always gets the same Breaker. Handing out a fresh one per call would reset the
// window on every request and never accumulate enough evidence to shed anything.
func (g *Group) For(node string) *Breaker {
	g.mu.Lock()
	defer g.mu.Unlock()

	if b, ok := g.breakers[node]; ok {
		return b
	}
	// New only fails on invalid options, which NewGroup already rejected.
	b, err := New(g.opts)
	if err != nil {
		panic("breaker: group options became invalid: " + err.Error())
	}
	g.breakers[node] = b
	return b
}

// Nodes returns the nodes this group has seen, and their current stats.
func (g *Group) Nodes() map[string]Stats {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := make(map[string]Stats, len(g.breakers))
	for node, b := range g.breakers {
		out[node] = b.Stats()
	}
	return out
}
