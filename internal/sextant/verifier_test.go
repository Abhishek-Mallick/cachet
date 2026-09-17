package sextant_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/sextant"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// The detection loop, driven deterministically. RunOnce is exported precisely so these tests do not
// sleep through intervals and hope: a verifier whose behaviour can only be observed in real time is
// one whose detection logic never gets tested properly, which for this component would mean the
// product's central claim rests on code nobody has exercised.

type fakeCache struct {
	mu      sync.Mutex
	entries map[string]uint64
	err     error
}

func (f *fakeCache) Peek(_ context.Context, key string) (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, false, f.err
	}
	v, ok := f.entries[key]
	return v, ok, nil
}

type fakeOrigin struct {
	mu   sync.Mutex
	rows map[string]uint64
}

func (f *fakeOrigin) Version(_ context.Context, key string) (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.rows[key]
	return v, ok, nil
}

func (f *fakeOrigin) Shard(string) (string, error) { return "shard0", nil }

type keyList struct {
	mu   sync.Mutex
	keys []string
	i    int
}

func (k *keyList) Next() (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.i >= len(k.keys) {
		return "", false
	}
	key := k.keys[k.i]
	k.i++
	return key, true
}

func (k *keyList) reset() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.i = 0
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newVerifier(t *testing.T, cache *fakeCache, origin *fakeOrigin, keys *keyList, clk *clock, onViolation func(sextant.Violation)) (*sextant.Verifier, *sextant.SLO) {
	t.Helper()

	slo := sextant.NewSLO(time.Hour)
	v, err := sextant.NewVerifier(sextant.VerifierOptions{
		Cache: cache, Origin: origin, Keys: keys,
		Bound: bound(), SLO: slo, Tracer: sextant.NewTracer(sextant.TracerOptions{}),
		Batch: 100, Now: clk.Now, OnViolation: onViolation,
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return v, slo
}

func TestAConsistentCacheProducesNoViolations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &clock{t: at(0)}
	cache := &fakeCache{entries: map[string]uint64{"entities:1": 100, "entities:2": 200}}
	origin := &fakeOrigin{rows: map[string]uint64{"entities:1": 100, "entities:2": 200}}
	keys := &keyList{keys: []string{"entities:1", "entities:2"}}

	v, slo := newVerifier(t, cache, origin, keys, clk, nil)
	round := v.RunOnce(ctx)

	if round.Violations != 0 {
		t.Errorf("Violations = %d on a consistent cache, want 0", round.Violations)
	}
	if round.Checked != 2 {
		t.Errorf("Checked = %d, want 2", round.Checked)
	}
	if r := slo.Report(consistency.Session, clk.Now()); !r.Known || r.Consistency != 1 {
		t.Errorf("SESSION = %+v, want a known figure of 1", r)
	}
}

func TestAnInFlightRaceIsNotReported(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &clock{t: at(0)}
	// Behind, but only just: an invalidation is in flight and the model never promised otherwise.
	cache := &fakeCache{entries: map[string]uint64{"entities:1": 100}}
	origin := &fakeOrigin{rows: map[string]uint64{"entities:1": 200}}
	keys := &keyList{keys: []string{"entities:1"}}

	v, slo := newVerifier(t, cache, origin, keys, clk, nil)
	round := v.RunOnce(ctx)

	if round.Violations != 0 {
		t.Errorf("Violations = %d for an entry behind for zero time, want 0", round.Violations)
	}
	// It is still an OBSERVATION: a clean one. Otherwise a system with in-flight races would have a
	// smaller denominator than one without, and its consistency figure would be computed from fewer
	// samples without anyone noticing.
	if r := slo.Report(consistency.Session, clk.Now()); r.Observations != 1 {
		t.Errorf("Observations = %d, want the in-flight race counted as a clean observation", r.Observations)
	}
}

func TestStalenessPastTheBoundIsReported(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &clock{t: at(0)}
	cache := &fakeCache{entries: map[string]uint64{"entities:1": 100}}
	origin := &fakeOrigin{rows: map[string]uint64{"entities:1": 200}}
	keys := &keyList{keys: []string{"entities:1"}}

	var found []sextant.Violation
	var mu sync.Mutex
	v, slo := newVerifier(t, cache, origin, keys, clk, func(vi sextant.Violation) {
		mu.Lock()
		defer mu.Unlock()
		found = append(found, vi)
	})

	// First round: records when the key was first seen behind. This is the sample that makes the
	// propagation bound a duration — one observation cannot tell a race from a permanent failure.
	v.RunOnce(ctx)

	clk.advance(bound().Duration() + time.Second)
	keys.reset()
	round := v.RunOnce(ctx)

	if round.Violations != 1 {
		t.Fatalf("Violations = %d after the bound elapsed, want 1", round.Violations)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(found) != 1 {
		t.Fatalf("OnViolation fired %d times, want 1", len(found))
	}
	if found[0].Key != "entities:1" || found[0].DBVersion != 200 {
		t.Errorf("the violation does not describe what was wrong: %+v", found[0])
	}
	if r := slo.Report(consistency.Eventual, clk.Now()); r.Violations != 1 {
		t.Errorf("EVENTUAL violations = %d, want 1", r.Violations)
	}
}

func TestCatchingUpClearsTheTimer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &clock{t: at(0)}
	cache := &fakeCache{entries: map[string]uint64{"entities:1": 100}}
	origin := &fakeOrigin{rows: map[string]uint64{"entities:1": 200}}
	keys := &keyList{keys: []string{"entities:1"}}

	v, _ := newVerifier(t, cache, origin, keys, clk, nil)
	v.RunOnce(ctx)

	// The entry catches up.
	cache.mu.Lock()
	cache.entries["entities:1"] = 200
	cache.mu.Unlock()
	clk.advance(bound().Duration() + time.Second)
	keys.reset()
	v.RunOnce(ctx)

	// It goes stale again, much later. The clock must start from THIS occasion: timing it from the
	// first would report a violation the instant an entry went stale, because the stored marker is
	// already older than the bound — turning every subsequent write into an immediate violation.
	cache.mu.Lock()
	cache.entries["entities:1"] = 200
	cache.mu.Unlock()
	origin.mu.Lock()
	origin.rows["entities:1"] = 300
	origin.mu.Unlock()
	keys.reset()

	round := v.RunOnce(ctx)
	if round.Violations != 0 {
		t.Errorf("Violations = %d immediately after an entry went stale again; the behind-timer was "+
			"not reset when it caught up", round.Violations)
	}
}

func TestAMissingEntryIsNotAViolation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &clock{t: at(0)}
	// Nothing cached is nothing to be wrong about: a miss costs a database read and no correctness.
	cache := &fakeCache{entries: map[string]uint64{}}
	origin := &fakeOrigin{rows: map[string]uint64{"entities:1": 200}}
	keys := &keyList{keys: []string{"entities:1"}}

	v, slo := newVerifier(t, cache, origin, keys, clk, nil)
	round := v.RunOnce(ctx)

	if round.Violations != 0 || round.Checked != 0 {
		t.Errorf("a missing entry produced %d violations and %d checks, want 0 and 0",
			round.Violations, round.Checked)
	}
	if r := slo.Report(consistency.Session, clk.Now()); r.Observations != 0 {
		t.Errorf("Observations = %d for a key that was not cached; a miss is not evidence about "+
			"consistency either way", r.Observations)
	}
}

func TestACacheErrorIsCountedNotFatal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &clock{t: at(0)}
	cache := &fakeCache{entries: map[string]uint64{}, err: errors.New("node down")}
	origin := &fakeOrigin{rows: map[string]uint64{"entities:1": 200}}
	keys := &keyList{keys: []string{"entities:1"}}

	v, slo := newVerifier(t, cache, origin, keys, clk, nil)
	round := v.RunOnce(ctx)

	// A verifier that stopped on the first error would go quiet during exactly the incident it
	// exists to observe. Counting the error keeps it running and makes the gap visible.
	if round.Errors != 1 {
		t.Errorf("Errors = %d, want 1", round.Errors)
	}
	// And the error must not be recorded as evidence of consistency. A verifier that could not read
	// the cache has observed nothing, and scoring it as clean would make an outage look healthy.
	if r := slo.Report(consistency.Session, clk.Now()); r.Observations != 0 {
		t.Errorf("Observations = %d after a cache error; a failed check is not a clean observation",
			r.Observations)
	}
}

func TestAViolationCarriesItsTrace(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &clock{t: at(0)}
	cache := &fakeCache{entries: map[string]uint64{"entities:1": 100}}
	origin := &fakeOrigin{rows: map[string]uint64{"entities:1": 200}}
	keys := &keyList{keys: []string{"entities:1"}}

	tracer := sextant.NewTracer(sextant.TracerOptions{})
	tracer.Record("entities:1", sextant.Event{Op: sextant.OpFill, Version: 100, At: at(0), Source: sextant.SourceReadFill})
	tracer.Record("entities:1", sextant.Event{Op: sextant.OpTombstone, Version: 200, At: at(1), Source: sextant.SourceCDC})

	slo := sextant.NewSLO(time.Hour)
	var got sextant.Violation
	v, err := sextant.NewVerifier(sextant.VerifierOptions{
		Cache: cache, Origin: origin, Keys: keys, Bound: bound(), SLO: slo,
		Tracer: tracer, Batch: 10, Now: clk.Now,
		OnViolation: func(vi sextant.Violation) { got = vi },
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	v.RunOnce(ctx)
	clk.advance(bound().Duration() + time.Second)
	keys.reset()
	v.RunOnce(ctx)

	// This is the difference between a monitor and a verifier. Without the trace the report says
	// "this was stale"; with it, it says which mutations happened and from which path — which is the
	// only form in which the number is actionable.
	if len(got.Trace) != 2 {
		t.Fatalf("the violation carries %d trace events, want 2", len(got.Trace))
	}
	if got.Trace[1].Source != sextant.SourceCDC {
		t.Errorf("trace event source = %v, want cdc", got.Trace[1].Source)
	}
}

func TestAVerifierRefusesToStartWithoutItsDependencies(t *testing.T) {
	t.Parallel()

	// Booting a verifier that cannot verify would produce an empty dashboard indistinguishable from
	// a healthy one.
	for name, opts := range map[string]sextant.VerifierOptions{
		"no cache":  {Origin: &fakeOrigin{}, Keys: &keyList{}, SLO: sextant.NewSLO(time.Hour)},
		"no origin": {Cache: &fakeCache{}, Keys: &keyList{}, SLO: sextant.NewSLO(time.Hour)},
		"no keys":   {Cache: &fakeCache{}, Origin: &fakeOrigin{}, SLO: sextant.NewSLO(time.Hour)},
		"no slo":    {Cache: &fakeCache{}, Origin: &fakeOrigin{}, Keys: &keyList{}},
	} {
		if _, err := sextant.NewVerifier(opts); err == nil {
			t.Errorf("NewVerifier accepted options with %s", name)
		}
	}
}

func TestRunStopsWhenTheContextIsCancelled(t *testing.T) {
	t.Parallel()

	clk := &clock{t: at(0)}
	cache := &fakeCache{entries: map[string]uint64{}}
	origin := &fakeOrigin{rows: map[string]uint64{}}
	keys := &keyList{}

	slo := sextant.NewSLO(time.Hour)
	v, err := sextant.NewVerifier(sextant.VerifierOptions{
		Cache: cache, Origin: origin, Keys: keys, Bound: bound(), SLO: slo,
		Interval: time.Millisecond, Batch: 1, Now: clk.Now,
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- v.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v on cancellation, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop when its context was cancelled; it would outlive its process")
	}
}

func TestShadowModeIsCarriedIntoTheVerifier(t *testing.T) {
	t.Parallel()

	// Shadow changes nothing about the arithmetic and everything about how the result should be
	// read, so it travels with the verifier rather than living in an operator's memory.
	slo := sextant.NewSLO(time.Hour)
	v, err := sextant.NewVerifier(sextant.VerifierOptions{
		Cache: &fakeCache{}, Origin: &fakeOrigin{}, Keys: &keyList{},
		Bound: bound(), SLO: slo, Shadow: true,
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if !v.Shadow() {
		t.Error("Shadow() = false on a verifier configured for shadow mode")
	}
}

func TestStatsAccumulateAcrossRounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &clock{t: at(0)}
	cache := &fakeCache{entries: map[string]uint64{}}
	origin := &fakeOrigin{rows: map[string]uint64{}}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("entities:%d", i)
		cache.entries[key] = 100
		origin.rows[key] = 100
	}
	keys := &keyList{keys: []string{"entities:0", "entities:1", "entities:2", "entities:3", "entities:4"}}

	v, _ := newVerifier(t, cache, origin, keys, clk, nil)
	v.RunOnce(ctx)
	keys.reset()
	v.RunOnce(ctx)

	s := v.Stats()
	if s.Rounds != 2 || s.Checked != 10 {
		t.Errorf("Stats = %+v, want 2 rounds and 10 checks", s)
	}
}
