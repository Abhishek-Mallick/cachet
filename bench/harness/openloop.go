package harness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Operation performs one request. i is the request's ordinal, so the caller can derive a key from
// it deterministically.
type Operation func(ctx context.Context, i uint64) error

// Driver issues requests at a fixed rate, independently of whether earlier ones have finished.
//
// This is the whole point of the harness. A closed-loop generator — send, wait for the reply, send
// the next — stops sending while the system is stalled, so a two-second stall produces one slow
// sample instead of two seconds' worth. The p99 then looks excellent precisely because the system
// was too slow to be measured (benchmarking doc §3.1).
//
// Here, request i is scheduled for runStart + i*interval and its latency is measured from that
// INTENDED start, not from when a worker picked it up. A backlog therefore appears in the
// distribution as the pile of late requests it actually is.
type Driver struct {
	// Rate is the target requests per second, held constant regardless of response times.
	Rate int

	// Workers is how many requests may be in flight at once.
	Workers int

	// Warmup is discarded. Cold caches, empty connection pools and a filling MyRocks block cache
	// are not the steady state, and the warmup duration is a declared parameter rather than a vibe
	// (benchmarking doc §3.3).
	Warmup time.Duration

	// Measure is the recorded window.
	Measure time.Duration

	Op Operation
}

// Result is one run's outcome.
type Result struct {
	Read *Recorder

	Requests       int64
	WarmupRequests int64
	Errors         int64

	// Behind counts requests dispatched later than their intended start by more than one interval.
	// It is the harness's own honesty check: a large value means the generator could not keep up,
	// so the run measured the generator rather than the system.
	Behind int64

	Elapsed    time.Duration
	Throughput float64
}

// Run executes the warmup and measurement phases.
func (d Driver) Run(ctx context.Context) (Result, error) {
	if d.Rate <= 0 {
		return Result{}, fmt.Errorf("harness: rate must be positive, got %d", d.Rate)
	}
	if d.Workers <= 0 {
		return Result{}, fmt.Errorf("harness: workers must be positive, got %d", d.Workers)
	}
	if d.Measure <= 0 {
		return Result{}, fmt.Errorf("harness: measure window must be positive, got %s", d.Measure)
	}
	if d.Op == nil {
		return Result{}, errors.New("harness: no operation")
	}

	interval := time.Second / time.Duration(d.Rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}

	var (
		requests, warmupReqs, failures, behind atomic.Int64
		nextIndex                              atomic.Uint64
	)

	recorders := make([]*Recorder, d.Workers)
	for i := range recorders {
		recorders[i] = NewRecorder()
	}

	type job struct {
		index         uint64
		intendedStart time.Time
		// measured is decided when the job is SCHEDULED, not when a worker finishes it. Deciding
		// it at completion means a warmup request that finishes after the phase boundary is
		// recorded as a measurement — and since warmup is exactly when the system is slowest, that
		// pollutes the distribution with the samples the warmup phase exists to exclude.
		measured bool
	}
	// The queue is small on purpose. A deep buffer would absorb a backlog inside the harness and
	// hide it from the measurement — the queueing delay belongs in the latency distribution, not in
	// a channel.
	jobs := make(chan job, d.Workers)

	var wg sync.WaitGroup
	for w := 0; w < d.Workers; w++ {
		wg.Add(1)
		go func(rec *Recorder) {
			defer wg.Done()
			for j := range jobs {
				err := d.Op(ctx, j.index)
				switch {
				case err != nil:
					failures.Add(1)
					// A fast failure is not a fast response. Letting errors into the histogram is
					// how a broken system reports excellent p99s.
				case j.measured:
					// Measured from the INTENDED start. This single line is what separates an
					// honest harness from a flattering one.
					rec.Record(time.Since(j.intendedStart))
					requests.Add(1)
				default:
					warmupReqs.Add(1)
				}
			}
		}(recorders[w])
	}

	runStart := time.Now()
	warmupEnd := runStart.Add(d.Warmup)
	measureEnd := warmupEnd.Add(d.Measure)

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

dispatch:
	for i := uint64(0); ; i++ {
		intended := runStart.Add(time.Duration(i) * interval)
		if !intended.Before(measureEnd) {
			break
		}
		measured := !intended.Before(warmupEnd)

		if wait := time.Until(intended); wait > 0 {
			timer.Reset(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				break dispatch
			}
		} else if wait < -interval {
			behind.Add(1)
		}

		select {
		case jobs <- job{index: nextIndex.Add(1) - 1, intendedStart: intended, measured: measured}:
		case <-ctx.Done():
			break dispatch
		}
	}

	close(jobs)
	wg.Wait()

	elapsed := time.Since(runStart)
	measured := elapsed - d.Warmup
	if measured <= 0 {
		measured = elapsed
	}

	merged := MergeRecorders(recorders...)
	return Result{
		Read:           merged,
		Requests:       requests.Load(),
		WarmupRequests: warmupReqs.Load(),
		Errors:         failures.Load(),
		Behind:         behind.Load(),
		Elapsed:        elapsed,
		Throughput:     float64(requests.Load()) / measured.Seconds(),
	}, ctx.Err()
}
