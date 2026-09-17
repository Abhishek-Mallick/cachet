package admission_test

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/admission"
)

// The milestone's stated exit criterion:
//
//	A write-churning key in a read-heavy table is demonstrably evicted from admission and stays
//	evicted, without oscillating.
//
// The last three words are the hard part, and the reason these tests simulate a workload over time
// rather than asserting a single decision. A policy is easy to get right on one sample and easy to
// get wrong across a thousand, and the failure mode — flipping — is invisible unless you look at the
// sequence.

func controller(clk *testClock) *admission.Controller {
	return admission.NewController(admission.ControllerOptions{
		Sketch: admission.NewSketch(admission.SketchOptions{Window: time.Minute, Buckets: 6, Now: clk.Now}),
		Policy: admission.NewPolicy(admission.PolicyOptions{
			AdmitRatio: 20, EvictRatio: 10, MinSamples: 50,
			MinDwell: 30 * time.Second, DefaultAdmit: true,
		}),
		Now: clk.Now,
	})
}

func TestAReadHeavyKeyStaysAdmitted(t *testing.T) {
	t.Parallel()

	clk := newClock(base())
	c := controller(clk)
	const key = "entities:hot"

	for round := 0; round < 40; round++ {
		for i := 0; i < 100; i++ {
			c.RecordRead(key)
		}
		c.RecordWrite(key)
		clk.advance(10 * time.Second)

		if !c.ShouldCache(key) {
			t.Fatalf("round %d evicted a 100:1 key: %s", round, c.Explain(key).Reason)
		}
	}
}

func TestAWriteChurningKeyIsEvictedAndStaysEvicted(t *testing.T) {
	t.Parallel()

	// The exit criterion itself. A key inside a read-heavy table that takes as many writes as reads:
	// every write pays invalidation, every read misses, and no human picking tables would ever find
	// it.
	clk := newClock(base())
	c := controller(clk)
	const key = "entities:churn"

	evictedAt := -1
	for round := 0; round < 60; round++ {
		for i := 0; i < 50; i++ {
			c.RecordRead(key)
			c.RecordWrite(key)
		}
		clk.advance(10 * time.Second)

		admitted := c.ShouldCache(key)
		switch {
		case evictedAt < 0 && !admitted:
			evictedAt = round
		case evictedAt >= 0 && admitted:
			t.Fatalf("round %d readmitted a key evicted at round %d; this is the oscillation the "+
				"hysteresis band and dwell exist to prevent: %s", round, evictedAt, c.Explain(key).Reason)
		}
	}

	if evictedAt < 0 {
		t.Fatal("a 1:1 key was never evicted")
	}
	t.Logf("evicted at round %d and stayed evicted for the remaining %d rounds", evictedAt, 60-evictedAt)
}

func TestABorderlineKeyDoesNotFlip(t *testing.T) {
	t.Parallel()

	// The case a single threshold gets wrong. This key hovers right around the boundary with random
	// jitter, which is what a real key near the threshold looks like — and with one threshold it
	// would flip on nearly every sample.
	clk := newClock(base())
	c := controller(clk)
	const key = "entities:borderline"
	rng := rand.New(rand.NewPCG(7, 11))

	flips := 0
	last := c.ShouldCache(key)
	for round := 0; round < 200; round++ {
		// Around 15:1, inside the 10–20 band, jittering across it.
		reads := 120 + rng.IntN(120) // 120..239
		for i := 0; i < reads; i++ {
			c.RecordRead(key)
		}
		for i := 0; i < 10; i++ {
			c.RecordWrite(key)
		}
		clk.advance(5 * time.Second)

		if got := c.ShouldCache(key); got != last {
			flips++
			last = got
		}
	}

	// A single-threshold policy on this input flips tens of times. The band plus the dwell should
	// keep it in single figures; anything more means the mitigations are not working.
	if flips > 5 {
		t.Errorf("a borderline key changed state %d times over 200 rounds; it is oscillating", flips)
	}
	t.Logf("borderline key changed state %d times over 200 rounds", flips)
}

func TestAKeyThatBecomesWriteHeavyIsEvictedThenReadmittedWhenItRecovers(t *testing.T) {
	t.Parallel()

	// Adaptive means adaptive in both directions. A key that becomes write-heavy during a migration
	// and goes back to being read-heavy afterwards must be cached again — otherwise the first bad
	// hour costs its hit rate permanently, and the mechanism is a one-way ratchet.
	clk := newClock(base())
	c := controller(clk)
	const key = "entities:phase"

	// Phase 1: read-heavy.
	for round := 0; round < 12; round++ {
		for i := 0; i < 200; i++ {
			c.RecordRead(key)
		}
		c.RecordWrite(key)
		clk.advance(10 * time.Second)
		c.ShouldCache(key)
	}
	if !c.ShouldCache(key) {
		t.Fatalf("precondition: a read-heavy key should be admitted: %s", c.Explain(key).Reason)
	}

	// Phase 2: write storm.
	for round := 0; round < 24; round++ {
		for i := 0; i < 100; i++ {
			c.RecordWrite(key)
		}
		for i := 0; i < 10; i++ {
			c.RecordRead(key)
		}
		clk.advance(10 * time.Second)
		c.ShouldCache(key)
	}
	if c.ShouldCache(key) {
		t.Fatalf("a key taking ten times more writes than reads stayed admitted: %s", c.Explain(key).Reason)
	}

	// Phase 3: back to read-heavy.
	for round := 0; round < 24; round++ {
		for i := 0; i < 200; i++ {
			c.RecordRead(key)
		}
		c.RecordWrite(key)
		clk.advance(10 * time.Second)
		c.ShouldCache(key)
	}
	if !c.ShouldCache(key) {
		t.Errorf("a recovered key was never readmitted, making admission a one-way ratchet: %s",
			c.Explain(key).Reason)
	}
}

func TestExplainMatchesTheDecision(t *testing.T) {
	t.Parallel()

	// A parallel implementation for the explanation would eventually disagree with the decision, and
	// the explanation is exactly the thing nobody would think to test against reality.
	clk := newClock(base())
	c := controller(clk)
	const key = "entities:explain"

	for i := 0; i < 100; i++ {
		c.RecordRead(key)
		c.RecordWrite(key)
	}
	clk.advance(time.Minute)

	d := c.Explain(key)
	if d.Admit != c.ShouldCache(key) {
		t.Error("Explain and ShouldCache disagree about the same key")
	}
	if d.Reason == "" {
		t.Error("Explain returned no reason")
	}
}

func TestAdmissionStateIsBounded(t *testing.T) {
	t.Parallel()

	// The sketch is bounded; without this the state alongside it is not, and the cost the sketch was
	// chosen to avoid comes back one layer up.
	clk := newClock(base())
	c := admission.NewController(admission.ControllerOptions{
		Sketch:        admission.NewSketch(admission.SketchOptions{Now: clk.Now}),
		Policy:        admission.NewPolicy(admission.PolicyOptions{DefaultAdmit: true}),
		StatesTracked: 100,
		Now:           clk.Now,
	})

	for i := 0; i < 5000; i++ {
		c.ShouldCache(fmt.Sprintf("entities:%d", i))
	}
	if got := c.TrackedKeys(); got > 100 {
		t.Errorf("TrackedKeys() = %d, over the configured bound of 100", got)
	}
}

func TestTheControllerIsSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	clk := newClock(base())
	c := controller(clk)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("entities:%d", i%40)
				c.RecordRead(key)
				if i%10 == 0 {
					c.RecordWrite(key)
				}
				c.ShouldCache(key)
				c.Explain(key)
			}
		}(g)
	}
	wg.Wait()
}
