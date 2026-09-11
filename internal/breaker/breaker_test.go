package breaker_test

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/breaker"
)

// fakeClock lets a test age the observation window without sleeping. A breaker whose behaviour can
// only be observed in real time is a breaker whose window logic never gets tested.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// sequenceRand returns a fixed ramp, so "shed 30% of traffic" can be asserted exactly rather than
// approximately. Real randomness would make the assertion flaky or force a tolerance so wide it
// would stop detecting a wrong probability.
func ramp(n int) func() float64 {
	i := 0
	return func() float64 {
		v := float64(i%n) / float64(n)
		i++
		return v
	}
}

func newBreaker(t *testing.T, opts breaker.Options) *breaker.Breaker {
	t.Helper()
	b, err := breaker.New(opts)
	if err != nil {
		t.Fatalf("breaker.New: %v", err)
	}
	return b
}

func defaults(c *fakeClock, r func() float64) breaker.Options {
	return breaker.Options{
		Window:       10 * time.Second,
		Buckets:      10,
		MinRequests:  20,
		FailureFloor: 0.05,
		MaxShed:      0.95,
		Now:          c.Now,
		Rand:         r,
	}
}

func TestRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	c := newClock()
	for _, tc := range []struct {
		name  string
		mutET func(*breaker.Options)
	}{
		{"zero window", func(o *breaker.Options) { o.Window = 0 }},
		{"zero buckets", func(o *breaker.Options) { o.Buckets = 0 }},
		{"negative floor", func(o *breaker.Options) { o.FailureFloor = -0.1 }},
		{"floor above one", func(o *breaker.Options) { o.FailureFloor = 1.5 }},
		{"max shed above one", func(o *breaker.Options) { o.MaxShed = 1.5 }},
		{"negative max shed", func(o *breaker.Options) { o.MaxShed = -0.1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := defaults(c, ramp(100))
			tc.mutET(&opts)
			if _, err := breaker.New(opts); err == nil {
				t.Errorf("New accepted %s", tc.name)
			}
		})
	}
}

func TestMaxShedOfOneIsRejected(t *testing.T) {
	t.Parallel()

	// Shedding 100% of traffic to a node means never calling it again, which means never learning
	// that it recovered. The breaker would latch open until the process restarted. A probe trickle
	// is what makes recovery self-detecting, so the configuration that removes it is refused.
	opts := defaults(newClock(), ramp(100))
	opts.MaxShed = 1.0

	if _, err := breaker.New(opts); err == nil {
		t.Error("New accepted MaxShed=1.0, which would latch the breaker open forever")
	}
}

func TestHealthyNodeIsNeverShed(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(100)))

	for i := 0; i < 500; i++ {
		b.Success()
	}
	if got := b.ShedProbability(); got != 0 {
		t.Errorf("ShedProbability() = %v on a fully healthy node, want 0", got)
	}
	for i := 0; i < 200; i++ {
		if !b.Allow() {
			t.Fatal("Allow() shed traffic to a fully healthy node")
		}
	}
}

func TestTooFewRequestsNeverSheds(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(100)))

	// Two failures out of two is a 100% failure rate and means nothing. Acting on it would let a
	// single blip shed real traffic.
	b.Failure()
	b.Failure()

	if got := b.ShedProbability(); got != 0 {
		t.Errorf("ShedProbability() = %v on 2 observations (MinRequests=20), want 0", got)
	}
}

func TestFailureRateBelowTheFloorIsNotShed(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(100)))

	// 2% failures against a 5% floor. Every healthy node has a nonzero error rate; treating it as
	// unhealthy would shed traffic permanently and for no gain.
	for i := 0; i < 98; i++ {
		b.Success()
	}
	b.Failure()
	b.Failure()

	if got := b.ShedProbability(); got != 0 {
		t.Errorf("ShedProbability() = %v at a 2%% failure rate under a 5%% floor, want 0", got)
	}
}

func TestShedIsProportionalToFailureRate(t *testing.T) {
	t.Parallel()

	// The property the product spec asks for: a node failing 40% of the time must keep serving the
	// majority of its traffic. A binary breaker trips fully open and sends 100% of that node's keys
	// to the database, turning a partial cache degradation into a total one.
	for _, tc := range []struct{ failures, total int }{
		{20, 100}, {40, 100}, {60, 100}, {80, 100},
	} {
		c := newClock()
		b := newBreaker(t, defaults(c, ramp(100)))
		for i := 0; i < tc.failures; i++ {
			b.Failure()
		}
		for i := 0; i < tc.total-tc.failures; i++ {
			b.Success()
		}

		rate := float64(tc.failures) / float64(tc.total)
		// Linear from the floor: everything above the tolerated rate is shed, rescaled so that a
		// 100% failure rate maps to MaxShed rather than to 1.
		want := (rate - 0.05) / (1 - 0.05) * 0.95
		got := b.ShedProbability()
		if math.Abs(got-want) > 0.01 {
			t.Errorf("at a %.0f%% failure rate ShedProbability() = %.3f, want %.3f", rate*100, got, want)
		}
		if got >= 1 {
			t.Errorf("at a %.0f%% failure rate the breaker sheds everything (%.3f)", rate*100, got)
		}
	}
}

func TestShedRisesMonotonicallyWithFailures(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(100)))
	for i := 0; i < 100; i++ {
		b.Success()
	}

	prev := b.ShedProbability()
	for i := 0; i < 200; i++ {
		b.Failure()
		got := b.ShedProbability()
		if got < prev {
			t.Fatalf("shed probability fell from %.3f to %.3f after another failure", prev, got)
		}
		prev = got
	}
}

func TestATotallyDeadNodeStillGetsProbes(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(1000)))

	for i := 0; i < 500; i++ {
		b.Failure()
	}

	if got := b.ShedProbability(); got > 0.95 {
		t.Errorf("ShedProbability() = %v on a dead node, want no more than MaxShed=0.95", got)
	}

	allowed := 0
	for i := 0; i < 1000; i++ {
		if b.Allow() {
			allowed++
		}
	}
	// Without a probe trickle the breaker can never observe recovery, and a node that came back
	// would stay shed until someone restarted the engine.
	if allowed == 0 {
		t.Fatal("a fully failing node received no probe traffic; recovery could never be detected")
	}
	if allowed > 100 {
		t.Errorf("a fully failing node received %d/1000 requests, want roughly 50", allowed)
	}
}

func TestShedProbabilityMatchesObservedShedRate(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(1000)))

	for i := 0; i < 50; i++ {
		b.Failure()
	}
	for i := 0; i < 50; i++ {
		b.Success()
	}

	want := b.ShedProbability()
	const trials = 1000
	shed := 0
	for i := 0; i < trials; i++ {
		if !b.Allow() {
			shed++
		}
	}

	// Allow() must actually shed at the rate it reports. If the two drifted apart, every metric and
	// every cachetctl explanation built on ShedProbability would be describing something the engine
	// does not do.
	got := float64(shed) / float64(trials)
	if math.Abs(got-want) > 0.02 {
		t.Errorf("Allow() shed %.3f of traffic, but ShedProbability() reports %.3f", got, want)
	}
}

func TestRecoveryClearsShedding(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(100)))

	for i := 0; i < 100; i++ {
		b.Failure()
	}
	if b.ShedProbability() == 0 {
		t.Fatal("precondition: a failing node should be shed")
	}

	// The window ages out. A breaker that never forgot would keep punishing a node for an outage
	// that ended hours ago.
	c.advance(11 * time.Second)

	if got := b.ShedProbability(); got != 0 {
		t.Errorf("ShedProbability() = %v after the window aged out, want 0", got)
	}
}

func TestOldFailuresLeaveTheWindowGradually(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(100)))

	for i := 0; i < 100; i++ {
		b.Failure()
	}
	start := b.ShedProbability()

	// Half the window passes with no traffic. The old failures are still inside it, so the node is
	// still considered unhealthy — shedding must not reset the moment a bucket rolls.
	c.advance(5 * time.Second)
	mid := b.ShedProbability()
	if mid == 0 {
		t.Error("shedding dropped to zero halfway through the window; old failures were forgotten early")
	}
	if mid > start {
		t.Errorf("shedding rose from %.3f to %.3f with no new failures", start, mid)
	}
}

func TestSuccessesAfterFailuresReduceShedding(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(100)))

	for i := 0; i < 100; i++ {
		b.Failure()
	}
	before := b.ShedProbability()

	for i := 0; i < 300; i++ {
		b.Success()
	}
	after := b.ShedProbability()

	if after >= before {
		t.Errorf("shed probability was %.3f before recovery traffic and %.3f after; it must fall", before, after)
	}
}

func TestStatsReportWhatTheBreakerSaw(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, defaults(c, ramp(100)))

	for i := 0; i < 30; i++ {
		b.Failure()
	}
	for i := 0; i < 70; i++ {
		b.Success()
	}

	// cachetctl has to explain WHY a node is being shed. "30 of 100 failed" is an explanation;
	// a bare probability is a number nobody can act on.
	s := b.Stats()
	if s.Failures != 30 || s.Successes != 70 || s.Total != 100 {
		t.Errorf("Stats() = %+v, want 30 failures / 70 successes / 100 total", s)
	}
	if math.Abs(s.FailureRate-0.30) > 0.001 {
		t.Errorf("Stats().FailureRate = %v, want 0.30", s.FailureRate)
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	t.Parallel()

	c := newClock()
	b := newBreaker(t, breaker.Options{
		Window: 10 * time.Second, Buckets: 10, MinRequests: 20,
		FailureFloor: 0.05, MaxShed: 0.95, Now: c.Now,
	})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				if (i+j)%3 == 0 {
					b.Failure()
				} else {
					b.Success()
				}
				b.Allow()
				b.ShedProbability()
			}
		}(i)
	}
	wg.Wait()

	if s := b.Stats(); s.Total != 16000 {
		t.Errorf("Stats().Total = %d after 16000 concurrent observations, want 16000", s.Total)
	}
}

func TestGroupKeepsNodesIndependent(t *testing.T) {
	t.Parallel()

	c := newClock()
	g, err := breaker.NewGroup(defaults(c, ramp(100)))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}

	sick := g.For("10.0.0.1:6379")
	healthy := g.For("10.0.0.2:6379")

	for i := 0; i < 100; i++ {
		sick.Failure()
		healthy.Success()
	}

	if sick.ShedProbability() == 0 {
		t.Error("the failing node is not being shed")
	}
	// One bad cache node must not cost the others their traffic. If it did, losing one node would
	// degrade the whole ring instead of a third of it.
	if got := healthy.ShedProbability(); got != 0 {
		t.Errorf("the healthy node's shed probability is %v; one node's failure leaked onto another", got)
	}
}

func TestGroupReturnsTheSameBreakerForANode(t *testing.T) {
	t.Parallel()

	c := newClock()
	g, err := breaker.NewGroup(defaults(c, ramp(100)))
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}

	// A Group handing out a fresh breaker per call would reset the window on every request and
	// never accumulate enough evidence to shed anything.
	first := g.For("node-a")
	second := g.For("node-a")
	if first != second {
		t.Error("For() returned a different breaker for the same node")
	}

	// Proven by behaviour as well as by identity: observations made through one handle must be
	// visible through the other.
	first.Failure()
	if got := second.Stats().Failures; got != 1 {
		t.Errorf("a failure recorded on one handle is invisible on the other (Failures=%d)", got)
	}
}

func TestGroupConcurrentAccessIsSafe(t *testing.T) {
	t.Parallel()

	c := newClock()
	g, err := breaker.NewGroup(breaker.Options{
		Window: 10 * time.Second, Buckets: 10, MinRequests: 20,
		FailureFloor: 0.05, MaxShed: 0.95, Now: c.Now,
	})
	if err != nil {
		t.Fatalf("NewGroup: %v", err)
	}

	nodes := []string{"a", "b", "c", "d"}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				b := g.For(nodes[j%len(nodes)])
				b.Success()
				b.Allow()
			}
		}(i)
	}
	wg.Wait()

	total := 0
	for _, n := range nodes {
		total += g.For(n).Stats().Total
	}
	if total != 8000 {
		t.Errorf("observed %d events across the group, want 8000", total)
	}
}
