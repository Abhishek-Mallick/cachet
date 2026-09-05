package harness_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/bench/harness"
)

func writeWorkload(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "w.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func TestWorkloadLoads(t *testing.T) {
	t.Parallel()

	w, err := harness.LoadWorkload(writeWorkload(t, `
id: W2
name: Realistic read-heavy
distribution: zipfian
theta: 0.99
read_fraction: 0.95
rows: 10000
rate: 500
workers: 16
warmup: 5s
measure: 10s
seed: 42
`))
	if err != nil {
		t.Fatalf("LoadWorkload: %v", err)
	}

	if w.ID != "W2" || w.Theta != 0.99 || w.ReadFraction != 0.95 {
		t.Errorf("loaded %+v", w)
	}
	if w.Warmup != 5*time.Second || w.Measure != 10*time.Second {
		t.Errorf("durations = %s/%s, want 5s/10s", w.Warmup, w.Measure)
	}
}

func TestWorkloadRejectsAnUnknownDistribution(t *testing.T) {
	t.Parallel()

	_, err := harness.LoadWorkload(writeWorkload(t, `
id: WX
distribution: gaussian
read_fraction: 0.95
rows: 100
rate: 10
workers: 1
measure: 1s
`))
	if err == nil {
		t.Error("LoadWorkload accepted an unknown distribution")
	}
}

func TestZipfianWorkloadRequiresTheta(t *testing.T) {
	t.Parallel()

	// θ is what makes W2 the primary workload rather than a second control. Defaulting it silently
	// would let a run claim to be skewed while being flat.
	_, err := harness.LoadWorkload(writeWorkload(t, `
id: W2
distribution: zipfian
read_fraction: 0.95
rows: 100
rate: 10
workers: 1
measure: 1s
`))
	if err == nil {
		t.Error("LoadWorkload accepted a zipfian workload with no theta")
	}
}

func TestUniformWorkloadRejectsTheta(t *testing.T) {
	t.Parallel()

	// Accepting and ignoring it would let W1 look configured for skew while producing flat traffic.
	_, err := harness.LoadWorkload(writeWorkload(t, `
id: W1
distribution: uniform
theta: 0.99
read_fraction: 0.95
rows: 100
rate: 10
workers: 1
measure: 1s
`))
	if err == nil {
		t.Error("LoadWorkload accepted theta on a uniform workload")
	}
}

func TestWorkloadValidatesTheReadFraction(t *testing.T) {
	t.Parallel()

	for _, frac := range []string{"-0.1", "1.5"} {
		_, err := harness.LoadWorkload(writeWorkload(t, `
id: WX
distribution: uniform
read_fraction: `+frac+`
rows: 100
rate: 10
workers: 1
measure: 1s
`))
		if err == nil {
			t.Errorf("LoadWorkload accepted read_fraction %s", frac)
		}
	}
}

func TestWorkloadBuildsItsGenerator(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ dist, extra string }{
		{"uniform", ""},
		{"zipfian", "theta: 0.99\n"},
		{"hot", ""},
	} {
		w, err := harness.LoadWorkload(writeWorkload(t, `
id: WX
distribution: `+tc.dist+`
`+tc.extra+`read_fraction: 1.0
rows: 1000
rate: 10
workers: 1
measure: 1s
`))
		if err != nil {
			t.Fatalf("LoadWorkload(%s): %v", tc.dist, err)
		}
		gen, err := w.Generator()
		if err != nil {
			t.Fatalf("Generator(%s): %v", tc.dist, err)
		}
		if v := gen.Next(); v >= w.Rows {
			t.Errorf("%s generator produced %d, outside [0,%d)", tc.dist, v, w.Rows)
		}
	}
}

func TestShippedWorkloadsAreValid(t *testing.T) {
	t.Parallel()

	// The catalogue in bench/workloads/ is what every published number is produced from. A file
	// that no longer parses would be discovered during a benchmark run rather than in CI.
	paths, err := filepath.Glob("../workloads/*.yaml")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no workload definitions found in bench/workloads/")
	}

	for _, p := range paths {
		w, err := harness.LoadWorkload(p)
		if err != nil {
			t.Errorf("%s: %v", filepath.Base(p), err)
			continue
		}
		if _, err := w.Generator(); err != nil {
			t.Errorf("%s: Generator: %v", filepath.Base(p), err)
		}
	}
}
