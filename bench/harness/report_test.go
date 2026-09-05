package harness_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/bench/harness"
)

func runWithP99(us int64) harness.RunMetrics {
	return harness.RunMetrics{
		Read:       harness.Latencies{P50: us / 4, P90: us / 2, P99: us, P999: us * 2, Count: 1000},
		Throughput: 1000,
	}
}

func TestAggregateReportsTheMedianNotTheMean(t *testing.T) {
	t.Parallel()

	// Percentiles do not average (benchmarking doc §3.2). These three runs have a mean p99 of
	// 40,666µs and a median of 12,000µs; reporting the mean would let one bad run dominate the
	// published number.
	agg := harness.Aggregate([]harness.RunMetrics{
		runWithP99(10_000), runWithP99(100_000), runWithP99(12_000),
	})

	if agg.Read.P99 != 12_000 {
		t.Errorf("aggregated p99 = %d, want the median 12000", agg.Read.P99)
	}
}

func TestAggregateReportsTheMinMaxSpread(t *testing.T) {
	t.Parallel()

	agg := harness.Aggregate([]harness.RunMetrics{
		runWithP99(10_000), runWithP99(100_000), runWithP99(12_000),
	})

	// Run-to-run variance on a developer machine is routinely 20–30%, which is larger than most of
	// the effects being measured. Publishing a number without its spread hides whether the effect
	// is real (benchmarking doc §3.4).
	if agg.Spread.ReadP99[0] != 10_000 || agg.Spread.ReadP99[1] != 100_000 {
		t.Errorf("spread = %v, want [10000 100000]", agg.Spread.ReadP99)
	}
}

func TestAggregateFlagsTooFewRuns(t *testing.T) {
	t.Parallel()

	// One run, one number is not a measurement. The result records that it is under-sampled rather
	// than quietly presenting itself as equivalent to a three-run result.
	if agg := harness.Aggregate([]harness.RunMetrics{runWithP99(10_000)}); agg.Sufficient {
		t.Error("a single run was reported as sufficient")
	}
	if agg := harness.Aggregate([]harness.RunMetrics{
		runWithP99(1), runWithP99(2), runWithP99(3),
	}); !agg.Sufficient {
		t.Error("three runs were reported as insufficient")
	}
}

func TestAggregateSumsErrorsAcrossRuns(t *testing.T) {
	t.Parallel()

	runs := []harness.RunMetrics{runWithP99(10), runWithP99(20), runWithP99(30)}
	runs[0].Errors = 3
	runs[2].Errors = 4

	if got := harness.Aggregate(runs).Errors; got != 7 {
		t.Errorf("errors = %d, want 7 — an error in any run must not disappear", got)
	}
}

func TestReportRoundTripsThroughJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	rep := harness.Report{
		SchemaVersion: 1,
		Phase:         "0-baseline",
		Workload:      "W2",
		Topology:      "service",
		Params:        harness.Params{WarmupSeconds: 60, MeasureSeconds: 300, Rate: 5000, Seed: 42, ZipfTheta: 0.99},
		Runs:          3,
		Client:        harness.Aggregate([]harness.RunMetrics{runWithP99(1), runWithP99(2), runWithP99(3)}),
	}

	path, err := rep.Save(dir)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !strings.HasPrefix(filepath.Base(path), "0-baseline_W2_") {
		t.Errorf("results file %q is not named <phase>_<workload>_<timestamp>.json", filepath.Base(path))
	}

	loaded, err := harness.LoadReports(dir)
	if err != nil {
		t.Fatalf("LoadReports: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded %d reports, want 1", len(loaded))
	}
	if loaded[0].Phase != "0-baseline" || loaded[0].Client.Read.P99 != 2 {
		t.Errorf("round trip lost data: %+v", loaded[0])
	}
}

func TestLoadReportsKeepsOnlyTheLatestPerPhaseAndWorkload(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	older := harness.Report{
		SchemaVersion: 1, Phase: "0-baseline", Workload: "W2", Runs: 3,
		RecordedAt: time.Now().Add(-time.Hour),
		Client:     harness.Aggregate([]harness.RunMetrics{runWithP99(9999)}),
	}
	newer := harness.Report{
		SchemaVersion: 1, Phase: "0-baseline", Workload: "W2", Runs: 3,
		RecordedAt: time.Now(),
		Client:     harness.Aggregate([]harness.RunMetrics{runWithP99(111)}),
	}
	mustSave(t, older, dir)
	mustSave(t, newer, dir)

	latest := harness.Latest(mustLoad(t, dir))

	// Rerunning a phase must replace its row rather than adding a second one, or the table becomes
	// a history nobody can read.
	if len(latest) != 1 {
		t.Fatalf("kept %d reports for one (phase, workload), want 1", len(latest))
	}
	if latest[0].Client.Read.P99 != 111 {
		t.Errorf("kept the older report (p99 %d)", latest[0].Client.Read.P99)
	}
}

func TestReadmeTableIsRegeneratedFromReports(t *testing.T) {
	t.Parallel()

	readme := filepath.Join(t.TempDir(), "README.md")
	writeReadme(t, readme, `# Cachet

Some prose.

| Configuration | Hit rate | p99 read | Origin QPS | Staleness | Cache mem |
|---|---|---|---|---|---|
| No cache | — | — | — | — | — |
| TTL only | — | — | — | — | — |

## Prior art
Trailing prose.
`)

	rep := harness.Report{
		SchemaVersion: 1, Phase: "0-baseline", Workload: "W2", Runs: 3,
		Client: harness.Aggregate([]harness.RunMetrics{
			runWithP99(1800), runWithP99(1900), runWithP99(1850),
		}),
		Origin: harness.OriginMetrics{QPS: 4512},
	}

	if err := harness.UpdateReadme(readme, []harness.Report{rep}); err != nil {
		t.Fatalf("UpdateReadme: %v", err)
	}

	out := readString(t, readme)

	// Nobody types a number into the README, ever (benchmarking doc §8). Hand-typed numbers drift,
	// and drift reads as dishonesty even when it is only laziness.
	if !strings.Contains(out, "1.85ms") {
		t.Errorf("the No cache row was not filled in with the median p99:\n%s", out)
	}
	if strings.Contains(out, "| No cache | — | — |") {
		t.Error("the No cache row is still all placeholders")
	}
	// Untouched rows and the surrounding prose must survive verbatim.
	if !strings.Contains(out, "| TTL only | — | — | — | — | — |") {
		t.Error("a phase with no results was overwritten instead of being left as a placeholder")
	}
	if !strings.Contains(out, "Some prose.") || !strings.Contains(out, "Trailing prose.") {
		t.Error("UpdateReadme damaged the surrounding document")
	}
}

func TestReadmeUpdateIsIdempotent(t *testing.T) {
	t.Parallel()

	readme := filepath.Join(t.TempDir(), "README.md")
	writeReadme(t, readme, `| Configuration | Hit rate | p99 read | Origin QPS | Staleness | Cache mem |
|---|---|---|---|---|---|
| No cache | — | — | — | — | — |
`)

	rep := harness.Report{
		SchemaVersion: 1, Phase: "0-baseline", Workload: "W2", Runs: 3,
		Client: harness.Aggregate([]harness.RunMetrics{runWithP99(1800)}),
	}

	if err := harness.UpdateReadme(readme, []harness.Report{rep}); err != nil {
		t.Fatalf("first UpdateReadme: %v", err)
	}
	first := readString(t, readme)

	if err := harness.UpdateReadme(readme, []harness.Report{rep}); err != nil {
		t.Fatalf("second UpdateReadme: %v", err)
	}
	// Running the generator twice must not produce a diff, or `make bench-report` becomes noise in
	// every pull request.
	if second := readString(t, readme); second != first {
		t.Error("UpdateReadme is not idempotent")
	}
}

func TestReadmeUpdateFailsLoudlyWithoutATable(t *testing.T) {
	t.Parallel()

	readme := filepath.Join(t.TempDir(), "README.md")
	writeReadme(t, readme, "# Cachet\n\nNo table here.\n")

	// Silently doing nothing would mean `make bench-report` reports success while the README keeps
	// showing placeholders.
	if err := harness.UpdateReadme(readme, nil); err == nil {
		t.Error("UpdateReadme succeeded on a README with no benchmark table")
	}
}

func mustSave(t *testing.T, r harness.Report, dir string) {
	t.Helper()
	if _, err := r.Save(dir); err != nil {
		t.Fatalf("Save: %v", err)
	}
	time.Sleep(2 * time.Millisecond) // distinct filenames
}

func mustLoad(t *testing.T, dir string) []harness.Report {
	t.Helper()
	rs, err := harness.LoadReports(dir)
	if err != nil {
		t.Fatalf("LoadReports: %v", err)
	}
	return rs
}

func writeReadme(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func TestReadmeRecordsWhereTheNumbersCameFrom(t *testing.T) {
	t.Parallel()

	readme := filepath.Join(t.TempDir(), "README.md")
	writeReadme(t, readme, `| Configuration | Hit rate | p99 read | Origin QPS | Staleness | Cache mem |
|---|---|---|---|---|---|
| No cache | — | — | — | — | — |

Trailing prose.
`)

	rep := harness.Report{
		SchemaVersion: 1, Phase: "0-baseline", Workload: "W2", Runs: 3,
		RecordedAt: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC),
		Env:        harness.Env{Host: "docker-desktop-macos", Arch: "arm64", CoresAvailable: 10},
		Params:     harness.Params{Rate: 500, MeasureSeconds: 20, ZipfTheta: 0.99},
		Client:     harness.Aggregate([]harness.RunMetrics{runWithP99(1800), runWithP99(1900), runWithP99(1850)}),
	}

	if err := harness.UpdateReadme(readme, []harness.Report{rep}); err != nil {
		t.Fatalf("UpdateReadme: %v", err)
	}
	out := readString(t, readme)

	// Docker Desktop on macOS runs a VM with a virtualised filesystem whose I/O latency is neither
	// predictable nor representative, and MyRocks is I/O-sensitive. Measuring there is fine;
	// implying otherwise is not (benchmarking doc §3.7).
	if !strings.Contains(out, "docker-desktop-macos") {
		t.Errorf("the README does not disclose the measurement environment:\n%s", out)
	}
	if !strings.Contains(out, "Trailing prose.") {
		t.Error("the provenance line displaced the surrounding document")
	}

	if err := harness.UpdateReadme(readme, []harness.Report{rep}); err != nil {
		t.Fatalf("second UpdateReadme: %v", err)
	}
	// Regenerating must replace the provenance line, not stack a second one under the table.
	if n := strings.Count(readString(t, readme), "docker-desktop-macos"); n != 1 {
		t.Errorf("provenance line appears %d times after two runs, want 1", n)
	}
}

func TestReadmeShowsMeasuredStaleness(t *testing.T) {
	t.Parallel()

	readme := filepath.Join(t.TempDir(), "README.md")
	writeReadme(t, readme, `| Configuration | Hit rate | p99 read | Origin QPS | Staleness | Cache mem |
|---|---|---|---|---|---|
| No cache | — | — | — | — | — |
| TTL only | — | — | — | — | — |
`)

	reports := []harness.Report{{
		SchemaVersion: 1, Phase: "1-ttl", Workload: "W2", Runs: 3,
		Env:    harness.Env{Host: "docker-desktop-macos"},
		Client: harness.Aggregate([]harness.RunMetrics{runWithP99(900), runWithP99(950), runWithP99(920)}),
		Cache:  harness.CacheMetrics{HitRate: 0.87},
		Staleness: harness.StalenessMetrics{
			Observed:     harness.Latencies{P50: 15_000_000, P99: 30_000_000, Max: 31_000_000, Count: 200},
			Observations: 200,
		},
	}}

	if err := harness.UpdateReadme(readme, reports); err != nil {
		t.Fatalf("UpdateReadme: %v", err)
	}
	out := readString(t, readme)

	// Phase 1's staleness is the number that motivates Phase 2. Leaving it as a placeholder while
	// the latency improves would advertise the win and hide the cost.
	if !strings.Contains(out, "30.00s") {
		t.Errorf("the TTL row does not show the measured p99 staleness:\n%s", out)
	}
	if !strings.Contains(out, "87.0%") {
		t.Errorf("the TTL row does not show the hit rate:\n%s", out)
	}
}

func TestReadmeMarksStalenessThatNeverConverged(t *testing.T) {
	t.Parallel()

	readme := filepath.Join(t.TempDir(), "README.md")
	writeReadme(t, readme, `| Configuration | Hit rate | p99 read | Origin QPS | Staleness | Cache mem |
|---|---|---|---|---|---|
| TTL only | — | — | — | — | — |
`)

	reports := []harness.Report{{
		SchemaVersion: 1, Phase: "1-ttl", Workload: "W2", Runs: 3,
		Env:    harness.Env{Host: "docker-desktop-macos"},
		Client: harness.Aggregate([]harness.RunMetrics{runWithP99(900)}),
		Staleness: harness.StalenessMetrics{
			Observed:     harness.Latencies{P99: 30_000_000, Count: 180},
			Observations: 200,
			NotConverged: 20,
		},
	}}

	if err := harness.UpdateReadme(readme, reports); err != nil {
		t.Fatalf("UpdateReadme: %v", err)
	}

	// A percentile computed only over the writes that DID become visible would be a flattering lie:
	// the ones that never converged are the worst cases, and dropping them improves the number.
	if out := readString(t, readme); !strings.Contains(out, "20 never converged") {
		t.Errorf("non-convergence was not disclosed:\n%s", out)
	}
}

func TestLatestMergesMeasurementsFromSeparateRuns(t *testing.T) {
	t.Parallel()

	// Latency and staleness are produced by different commands and land in different files. Keeping
	// only the newest file per (phase, workload) would let a staleness probe silently erase the
	// latency numbers for the same phase — the row would lose the column that was measured first.
	latency := harness.Report{
		SchemaVersion: 1, Phase: "1-ttl", Workload: "W2", Runs: 3,
		RecordedAt: time.Now().Add(-time.Minute),
		Client:     harness.Aggregate([]harness.RunMetrics{runWithP99(1200), runWithP99(1300), runWithP99(1250)}),
		Cache:      harness.CacheMetrics{HitRate: 0.81},
		Origin:     harness.OriginMetrics{QPS: 139},
	}
	staleness := harness.Report{
		SchemaVersion: 1, Phase: "1-ttl", Workload: "W2", Runs: 1,
		RecordedAt: time.Now(),
		Staleness: harness.StalenessMetrics{
			Observed: harness.Latencies{P99: 10_100_000, Count: 12}, Observations: 12,
		},
	}

	latest := harness.Latest([]harness.Report{latency, staleness})
	if len(latest) != 1 {
		t.Fatalf("kept %d reports, want 1", len(latest))
	}

	got := latest[0]
	if got.Staleness.Observations != 12 {
		t.Error("the newer staleness measurement was lost")
	}
	if got.Client.Read.P99 != 1250 {
		t.Errorf("client p99 = %d, want 1250 — the older latency measurement was erased", got.Client.Read.P99)
	}
	if got.Cache.HitRate != 0.81 || got.Origin.QPS != 139 {
		t.Errorf("cache/origin measurements were lost: %v / %v", got.Cache.HitRate, got.Origin.QPS)
	}
}

func TestLatestPrefersTheNewerOfTwoOverlappingMeasurements(t *testing.T) {
	t.Parallel()

	older := harness.Report{
		SchemaVersion: 1, Phase: "1-ttl", Workload: "W2", Runs: 3,
		RecordedAt: time.Now().Add(-time.Hour),
		Client:     harness.Aggregate([]harness.RunMetrics{runWithP99(9999)}),
	}
	newer := harness.Report{
		SchemaVersion: 1, Phase: "1-ttl", Workload: "W2", Runs: 3,
		RecordedAt: time.Now(),
		Client:     harness.Aggregate([]harness.RunMetrics{runWithP99(111)}),
	}

	// Merging must never resurrect a superseded number. Re-running a phase replaces what it
	// re-measured; it only fills in what it did not measure at all.
	if got := harness.Latest([]harness.Report{older, newer})[0].Client.Read.P99; got != 111 {
		t.Errorf("client p99 = %d, want the newer 111", got)
	}
}
