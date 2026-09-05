// Package harness is Cachet's benchmark load generator.
//
// It is built in Phase 0, before any caching code exists, for a reason stated plainly in
// docs/cachet-benchmarking.md §1: a baseline collected after building the thing you want to look
// good is not a baseline, it is a rationalisation.
package harness

import (
	"fmt"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	// minLatencyUs and maxLatencyUs bound the histogram: 1µs to 5 minutes. A value outside the
	// range is dropped by HdrHistogram, so the ceiling is deliberately far above any plausible
	// response — losing the very outliers a stampede produces would defeat the purpose.
	minLatencyUs = 1
	maxLatencyUs = 300 * 1000 * 1000

	// sigfigs is HdrHistogram's precision. Three significant figures keeps p99.9 trustworthy while
	// holding memory to a few hundred KB per recorder.
	sigfigs = 3
)

// Recorder accumulates latency samples for one worker.
//
// Each worker records into its own Recorder and the harness merges them at the end. That ordering
// matters: percentiles cannot be averaged, so merging the DISTRIBUTIONS and computing the
// percentile once from the merged data is the only correct way to combine workers or runs
// (benchmarking doc §3.2).
type Recorder struct {
	hist *hdrhistogram.Histogram
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{hist: hdrhistogram.New(minLatencyUs, maxLatencyUs, sigfigs)}
}

// Record adds one latency sample.
//
// d must be measured from the request's INTENDED start time, not from when a worker got to it.
// Recording the latter is what produces coordinated omission.
//
// Note what this deliberately does NOT do: HdrHistogram's RecordCorrectedValue back-fills synthetic
// samples for requests a stall prevented from being issued. That correction exists for CLOSED-loop
// harnesses, which genuinely skip requests during a stall. This harness is open-loop and issues
// every scheduled request, measuring each from its intended start — so the late requests are
// already present as real samples, and applying the correction on top of them would count the same
// backlog twice and inflate the tail.
//
// The residual case the correction would have covered is a generator that could not keep up and
// never dispatched some requests at all. That is not corrected silently; it is reported as
// Result.Behind, because a run where the harness was the bottleneck measured the harness rather
// than the system and should be discarded rather than adjusted.
func (r *Recorder) Record(d time.Duration) {
	us := d.Microseconds()
	if us < minLatencyUs {
		us = minLatencyUs
	}
	if us > maxLatencyUs {
		us = maxLatencyUs
	}
	_ = r.hist.RecordValue(us)
}

// Merge folds another Recorder's distribution into this one.
func (r *Recorder) Merge(other *Recorder) {
	if other == nil {
		return
	}
	r.hist.Merge(other.hist)
}

// Count returns the number of recorded samples.
func (r *Recorder) Count() int64 { return r.hist.TotalCount() }

// Percentile returns the latency at the given percentile, computed from the distribution.
func (r *Recorder) Percentile(p float64) time.Duration {
	return time.Duration(r.hist.ValueAtQuantile(p)) * time.Microsecond
}

// Max returns the largest recorded sample.
func (r *Recorder) Max() time.Duration {
	return time.Duration(r.hist.Max()) * time.Microsecond
}

// Mean returns the arithmetic mean. It is a diagnostic, never a headline: an average hides exactly
// the tail this project is measured on.
func (r *Recorder) Mean() time.Duration {
	return time.Duration(r.hist.Mean()) * time.Microsecond
}

// CountAbove returns how many samples exceeded d. It is how a test asserts that a stall produced a
// backlog rather than a single slow sample.
func (r *Recorder) CountAbove(d time.Duration) int64 {
	var n int64
	for _, bar := range r.hist.Distribution() {
		if time.Duration(bar.From)*time.Microsecond > d {
			n += bar.Count
		}
	}
	return n
}

// MergeRecorders combines several recorders into one distribution.
func MergeRecorders(rs ...*Recorder) *Recorder {
	out := NewRecorder()
	for _, r := range rs {
		out.Merge(r)
	}
	return out
}

// Latencies is the percentile summary written to a results file.
type Latencies struct {
	P50   int64 `json:"p50_us"`
	P90   int64 `json:"p90_us"`
	P99   int64 `json:"p99_us"`
	P999  int64 `json:"p999_us"`
	Max   int64 `json:"max_us"`
	Mean  int64 `json:"mean_us"`
	Count int64 `json:"count"`
}

// Summarise computes the percentile summary from the merged distribution.
func (r *Recorder) Summarise() Latencies {
	return Latencies{
		P50:   r.Percentile(50).Microseconds(),
		P90:   r.Percentile(90).Microseconds(),
		P99:   r.Percentile(99).Microseconds(),
		P999:  r.Percentile(99.9).Microseconds(),
		Max:   r.Max().Microseconds(),
		Mean:  r.Mean().Microseconds(),
		Count: r.Count(),
	}
}

// String renders a one-line summary for a terminal.
func (l Latencies) String() string {
	return fmt.Sprintf("p50=%s p90=%s p99=%s p99.9=%s max=%s n=%d",
		time.Duration(l.P50)*time.Microsecond,
		time.Duration(l.P90)*time.Microsecond,
		time.Duration(l.P99)*time.Microsecond,
		time.Duration(l.P999)*time.Microsecond,
		time.Duration(l.Max)*time.Microsecond,
		l.Count)
}
