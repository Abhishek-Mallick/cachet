package harness_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/bench/harness"
)

// TestOpenLoopExposesAStallThatClosedLoopWouldHide is the single most important test in the
// benchmark harness, and the reason the harness is written before any caching code exists.
//
// A CLOSED-loop generator sends a request, waits for the reply, then sends the next. When the system
// stalls for 300ms it simply stops sending, so the stall produces exactly ONE slow sample and the
// p99 barely moves. The system looks fast precisely because it was too slow to be measured. That is
// coordinated omission (benchmarking doc §3.1).
//
// It would be catastrophic here specifically: stampedes and invalidation storms are stall-shaped,
// and they are exactly what Cachet claims to fix. A closed-loop harness would hide the problem and
// then hide the fix.
//
// So latency is measured from the time a request was SUPPOSED to start. One 300ms stall at 1000 rps
// must show up as hundreds of slow samples — every request that was due during the stall — not one.
func TestOpenLoopExposesAStallThatClosedLoopWouldHide(t *testing.T) {
	t.Parallel()

	const (
		rate      = 1000
		stall     = 300 * time.Millisecond
		stallAt   = 100
		totalReqs = 600
	)

	var issued atomic.Int64
	d := harness.Driver{
		Rate:    rate,
		Workers: 1, // one worker, so the stall genuinely blocks the pipeline
		Measure: time.Duration(totalReqs) * time.Second / rate,
		Op: func(context.Context, uint64) error {
			if issued.Add(1) == stallAt {
				time.Sleep(stall)
			}
			return nil
		},
	}

	res, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	slow := res.Read.CountAbove(100 * time.Millisecond)
	// A closed-loop harness reports 1 here. Open-loop must report the whole backlog.
	if slow < 50 {
		t.Errorf("only %d samples exceeded 100ms after a %s stall at %d rps; "+
			"coordinated omission is not being corrected", slow, stall, rate)
	}
	if res.Read.Percentile(99) < 50*time.Millisecond {
		t.Errorf("p99 = %s after a %s stall; the stall was omitted from the distribution",
			res.Read.Percentile(99), stall)
	}
	t.Logf("stall of %s at %d rps produced %d samples over 100ms, p99 %s",
		stall, rate, slow, res.Read.Percentile(99))
}

func TestDriverIssuesTheRequestedNumberOfRequests(t *testing.T) {
	t.Parallel()

	const rate = 500
	var count atomic.Int64

	d := harness.Driver{
		Rate:    rate,
		Workers: 4,
		Measure: 200 * time.Millisecond,
		Op:      func(context.Context, uint64) error { count.Add(1); return nil },
	}

	res, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// 500 rps for 200ms is 100 requests. Allow slack for scheduler granularity, but a harness that
	// silently issues half of what it claims makes every throughput number fiction.
	if got := count.Load(); got < 80 || got > 120 {
		t.Errorf("issued %d requests, want ~100", got)
	}
	// Every issued request is recorded exactly once. A sample count that exceeds the request count
	// would mean synthetic samples are being invented somewhere.
	if res.Read.Count() != res.Requests {
		t.Errorf("recorded %d samples for %d measured requests", res.Read.Count(), res.Requests)
	}
}

func TestWarmupSamplesAreDiscarded(t *testing.T) {
	t.Parallel()

	// "Cold" behaviour is keyed off the request index rather than a wall-clock goroutine, so the
	// phase boundary is deterministic. A racing timer would make this test flaky for reasons that
	// have nothing to do with the code under test.
	//
	// 150ms of warmup at 200rps is ~30 requests, so indices below 25 are safely inside it. The rate
	// is also within what 8 workers can sustain while cold (8/20ms = 400rps), because an
	// over-saturated scenario would starve the measurement window and the test would pass vacuously.
	const coldRequests = 25

	d := harness.Driver{
		Rate:    200,
		Workers: 8,
		Warmup:  150 * time.Millisecond,
		Measure: 150 * time.Millisecond,
		Op: func(_ context.Context, i uint64) error {
			if i < coldRequests {
				time.Sleep(20 * time.Millisecond)
			}
			return nil
		},
	}

	res, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Cold caches, empty pools and a filling MyRocks block cache are not the steady state
	// (benchmarking doc §3.3). Warmup is a declared parameter, and its samples must not reach the
	// recorded distribution.
	if p99 := res.Read.Percentile(99); p99 > 10*time.Millisecond {
		t.Errorf("p99 = %s; warmup samples leaked into the measurement window", p99)
	}
	if res.WarmupRequests == 0 {
		t.Error("no warmup requests were issued, so the phase separation was not exercised")
	}
	if res.Read.Count() == 0 {
		t.Fatal("no measurement samples were recorded; the assertion above proved nothing")
	}
}

func TestErrorsAreCountedAndDoNotStopTheRun(t *testing.T) {
	t.Parallel()

	var n atomic.Int64
	d := harness.Driver{
		Rate:    500,
		Workers: 2,
		Measure: 200 * time.Millisecond,
		Op: func(context.Context, uint64) error {
			if n.Add(1)%10 == 0 {
				return errors.New("boom")
			}
			return nil
		},
	}

	res, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// A run that aborts on the first error cannot measure behaviour under fault injection, which is
	// the entire point of the chaos suite later.
	if res.Errors == 0 {
		t.Error("errors were not counted")
	}
	if res.Read.Count() == 0 {
		t.Error("the run produced no samples")
	}
}

func TestFailedRequestsAreExcludedFromLatency(t *testing.T) {
	t.Parallel()

	d := harness.Driver{
		Rate:    500,
		Workers: 2,
		Measure: 150 * time.Millisecond,
		Op: func(context.Context, uint64) error {
			time.Sleep(30 * time.Millisecond)
			return errors.New("boom")
		},
	}

	res, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// A fast failure is not a fast response. Letting errors into the latency histogram is how a
	// broken system reports excellent p99s.
	if res.Read.Count() != 0 {
		t.Errorf("%d failed requests were recorded as latency samples", res.Read.Count())
	}
	if res.Errors == 0 {
		t.Error("errors were not counted")
	}
}

func TestDriverStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	d := harness.Driver{
		Rate:    1000,
		Workers: 2,
		Measure: 30 * time.Second, // far longer than the context allows
		Op:      func(context.Context, uint64) error { return nil },
	}

	start := time.Now()
	if _, err := d.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Run took %s after its context expired", elapsed)
	}
}

func TestDriverValidatesItsParameters(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		d    harness.Driver
	}{
		{"no rate", harness.Driver{Workers: 1, Measure: time.Second, Op: noop}},
		{"no workers", harness.Driver{Rate: 10, Measure: time.Second, Op: noop}},
		{"no measure window", harness.Driver{Rate: 10, Workers: 1, Op: noop}},
		{"no operation", harness.Driver{Rate: 10, Workers: 1, Measure: time.Second}},
	} {
		if _, err := tc.d.Run(context.Background()); err == nil {
			t.Errorf("Run accepted a driver with %s", tc.name)
		}
	}
}

func noop(context.Context, uint64) error { return nil }
