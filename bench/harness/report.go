package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// SchemaVersion is the results-file format version. It is written into every file so that a report
// produced today is still readable when the schema grows.
const SchemaVersion = 1

// minimumRuns is the smallest number of runs that counts as a measurement.
//
// Run-to-run variance on a developer machine is routinely 20–30%, larger than most of the effects
// being measured. One run is one number, not a result (benchmarking doc §3.4).
const minimumRuns = 3

// RunMetrics is one run's client-side outcome.
type RunMetrics struct {
	Read       Latencies `json:"read"`
	Write      Latencies `json:"write"`
	Throughput float64   `json:"throughput_rps"`
	Errors     int64     `json:"errors"`
	Behind     int64     `json:"behind"`
}

// Spread records the min and max of a metric across runs.
type Spread struct {
	ReadP99 [2]int64 `json:"read_p99_us"`
}

// ClientMetrics is the aggregate across runs.
type ClientMetrics struct {
	Read       Latencies `json:"read"`
	Write      Latencies `json:"write"`
	Throughput float64   `json:"throughput_rps"`
	Errors     int64     `json:"errors"`
	Behind     int64     `json:"behind"`
	Spread     Spread    `json:"spread"`

	// Sufficient is false when fewer than three runs were aggregated. It travels with the numbers
	// so an under-sampled result cannot be mistaken for a measured one further downstream.
	Sufficient bool `json:"sufficient"`
	Runs       int  `json:"runs"`
}

// OriginMetrics is the database-side cost, which is the metric that actually matters for the
// caching claim: client latency may barely move while origin load collapses (benchmarking doc §3.6).
type OriginMetrics struct {
	QPS     float64 `json:"qps"`
	PeakQPS float64 `json:"peak_qps"`
}

// CacheMetrics is the cache-side outcome. Empty until Phase 1, when a cache exists.
type CacheMetrics struct {
	HitRate     float64 `json:"hit_rate"`
	MemoryBytes int64   `json:"memory_bytes"`
}

// Params records the run parameters, so a number can be reproduced rather than merely believed.
type Params struct {
	WarmupSeconds  int     `json:"warmup_s"`
	MeasureSeconds int     `json:"measure_s"`
	Rate           int     `json:"rate"`
	Workers        int     `json:"workers"`
	Seed           int64   `json:"seed"`
	ZipfTheta      float64 `json:"zipf_theta"`
	Rows           uint64  `json:"rows"`
	ReadFraction   float64 `json:"read_fraction"`
}

// Env records where a number was produced.
//
// Docker Desktop on macOS runs a VM with a virtualised filesystem whose I/O latency is neither
// predictable nor representative, and MyRocks is I/O-sensitive. Measuring there is fine; implying
// otherwise is not, so the host is recorded in every file and stated in the README
// (benchmarking doc §3.7, §4).
type Env struct {
	Host           string            `json:"host"`
	OS             string            `json:"os"`
	Arch           string            `json:"arch"`
	CoresAvailable int               `json:"cores_available"`
	GoVersion      string            `json:"go_version"`
	Images         map[string]string `json:"images,omitempty"`
	CachetCommit   string            `json:"cachet_commit,omitempty"`
}

// Report is one results file.
type Report struct {
	SchemaVersion int       `json:"schema_version"`
	Phase         string    `json:"phase"`
	Workload      string    `json:"workload"`
	Topology      string    `json:"topology"`
	RecordedAt    time.Time `json:"recorded_at"`

	Env    Env    `json:"env"`
	Params Params `json:"params"`
	Runs   int    `json:"runs"`

	Client ClientMetrics `json:"client"`
	Cache  CacheMetrics  `json:"cache"`
	Origin OriginMetrics `json:"origin"`
}

// Aggregate combines several runs into one published result.
//
// The percentile for each run is computed from that run's merged histogram, and the runs are then
// combined by MEDIAN — never by mean. Percentiles do not average: three runs at 10ms, 100ms and
// 12ms have a mean of 40ms, which describes none of them and lets a single bad run set the
// published number (benchmarking doc §3.2, §3.4).
func Aggregate(runs []RunMetrics) ClientMetrics {
	out := ClientMetrics{Runs: len(runs), Sufficient: len(runs) >= minimumRuns}
	if len(runs) == 0 {
		return out
	}

	out.Read = medianLatencies(runs, func(r RunMetrics) Latencies { return r.Read })
	out.Write = medianLatencies(runs, func(r RunMetrics) Latencies { return r.Write })
	out.Throughput = medianFloat(collectFloat(runs, func(r RunMetrics) float64 { return r.Throughput }))

	p99s := collectInt(runs, func(r RunMetrics) int64 { return r.Read.P99 })
	sort.Slice(p99s, func(i, j int) bool { return p99s[i] < p99s[j] })
	out.Spread.ReadP99 = [2]int64{p99s[0], p99s[len(p99s)-1]}

	// Errors and backlog are SUMMED, not medianed: an error in any run is a fact about the system,
	// and taking the middle value would let two clean runs erase a broken one.
	for _, r := range runs {
		out.Errors += r.Errors
		out.Behind += r.Behind
	}
	return out
}

func medianLatencies(runs []RunMetrics, pick func(RunMetrics) Latencies) Latencies {
	return Latencies{
		P50:   medianInt(collectInt(runs, func(r RunMetrics) int64 { return pick(r).P50 })),
		P90:   medianInt(collectInt(runs, func(r RunMetrics) int64 { return pick(r).P90 })),
		P99:   medianInt(collectInt(runs, func(r RunMetrics) int64 { return pick(r).P99 })),
		P999:  medianInt(collectInt(runs, func(r RunMetrics) int64 { return pick(r).P999 })),
		Max:   medianInt(collectInt(runs, func(r RunMetrics) int64 { return pick(r).Max })),
		Mean:  medianInt(collectInt(runs, func(r RunMetrics) int64 { return pick(r).Mean })),
		Count: sumInt(collectInt(runs, func(r RunMetrics) int64 { return pick(r).Count })),
	}
}

func collectInt(runs []RunMetrics, pick func(RunMetrics) int64) []int64 {
	out := make([]int64, 0, len(runs))
	for _, r := range runs {
		out = append(out, pick(r))
	}
	return out
}

func collectFloat(runs []RunMetrics, pick func(RunMetrics) float64) []float64 {
	out := make([]float64, 0, len(runs))
	for _, r := range runs {
		out = append(out, pick(r))
	}
	return out
}

func medianInt(vs []int64) int64 {
	if len(vs) == 0 {
		return 0
	}
	sort.Slice(vs, func(i, j int) bool { return vs[i] < vs[j] })
	return vs[len(vs)/2]
}

func medianFloat(vs []float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	sort.Float64s(vs)
	return vs[len(vs)/2]
}

func sumInt(vs []int64) int64 {
	var total int64
	for _, v := range vs {
		total += v
	}
	return total
}

// CurrentEnv captures the machine this run happened on.
func CurrentEnv(host string, images map[string]string, commit string) Env {
	return Env{
		Host:           host,
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		CoresAvailable: runtime.NumCPU(),
		GoVersion:      runtime.Version(),
		Images:         images,
		CachetCommit:   commit,
	}
}

// Save writes the report to dir as <phase>_<workload>_<timestamp>.json.
func (r Report) Save(dir string) (string, error) {
	if r.SchemaVersion == 0 {
		r.SchemaVersion = SchemaVersion
	}
	if r.RecordedAt.IsZero() {
		r.RecordedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("harness: create %s: %w", dir, err)
	}

	name := fmt.Sprintf("%s_%s_%s.json", r.Phase, r.Workload, r.RecordedAt.UTC().Format("20060102T150405.000"))
	path := filepath.Join(dir, name)

	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", fmt.Errorf("harness: encode report: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return "", fmt.Errorf("harness: write %s: %w", path, err)
	}
	return path, nil
}

// LoadReports reads every results file in dir.
func LoadReports(dir string) ([]Report, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("harness: read %s: %w", dir, err)
	}

	var out []Report
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("harness: read %s: %w", path, err)
		}
		var r Report
		if err := json.Unmarshal(data, &r); err != nil {
			return nil, fmt.Errorf("harness: parse %s: %w", path, err)
		}
		out = append(out, r)
	}
	return out, nil
}

// Latest keeps only the most recent report per (phase, workload).
//
// Rerunning a phase replaces its row rather than adding a second one; otherwise the table becomes a
// history nobody can read.
func Latest(reports []Report) []Report {
	best := make(map[string]Report, len(reports))
	for _, r := range reports {
		key := r.Phase + "/" + r.Workload
		if cur, ok := best[key]; !ok || r.RecordedAt.After(cur.RecordedAt) {
			best[key] = r
		}
	}

	out := make([]Report, 0, len(best))
	for _, r := range best {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Phase < out[j].Phase })
	return out
}
