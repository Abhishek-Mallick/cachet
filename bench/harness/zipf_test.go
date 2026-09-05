package harness_test

import (
	"sort"
	"testing"

	"github.com/Abhishek-Mallick/cachet/bench/harness"
)

func TestZipfianRejectsInvalidParameters(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		items uint64
		theta float64
	}{
		{"no items", 0, 0.99},
		{"negative theta", 100, -0.1},
		{"theta of one", 100, 1.0}, // the exponent 1/(1-theta) is undefined at exactly 1
	} {
		if _, err := harness.NewZipfian(tc.items, tc.theta, 42); err == nil {
			t.Errorf("NewZipfian accepted %s", tc.name)
		}
	}
}

func TestZipfianStaysInRange(t *testing.T) {
	t.Parallel()

	const items = 10_000
	z, err := harness.NewZipfian(items, 0.99, 42)
	if err != nil {
		t.Fatalf("NewZipfian: %v", err)
	}

	for i := 0; i < 100_000; i++ {
		if v := z.Next(); v >= items {
			t.Fatalf("Next() = %d, outside [0,%d)", v, items)
		}
	}
}

func TestZipfianIsDeterministicForASeed(t *testing.T) {
	t.Parallel()

	// Benchmark rows are only comparable if every run replays the same key sequence
	// (benchmarking doc §5). A generator that drifts between runs makes every row in the table a
	// different experiment.
	a, err := harness.NewZipfian(100_000, 0.99, 42)
	if err != nil {
		t.Fatalf("NewZipfian: %v", err)
	}
	b, err := harness.NewZipfian(100_000, 0.99, 42)
	if err != nil {
		t.Fatalf("NewZipfian: %v", err)
	}

	for i := 0; i < 10_000; i++ {
		if x, y := a.Next(), b.Next(); x != y {
			t.Fatalf("sequences diverged at %d: %d vs %d", i, x, y)
		}
	}
}

func TestZipfianConcentratesTrafficOnFewKeys(t *testing.T) {
	t.Parallel()

	const (
		items   = 100_000
		samples = 500_000
	)
	z, err := harness.NewZipfian(items, 0.99, 42)
	if err != nil {
		t.Fatalf("NewZipfian: %v", err)
	}

	counts := make(map[uint64]int, items)
	for i := 0; i < samples; i++ {
		counts[z.Next()]++
	}

	// Uniform keys over ten million rows means almost no key is ever hot, the hit rate is near
	// zero, and every caching effect vanishes — including the stampede this project claims to fix
	// (benchmarking doc §3.5). So skew is a property the workload must actually have.
	top := topShare(counts, samples, items/100) // the hottest 1% of keys
	if top < 0.25 {
		t.Errorf("the hottest 1%% of keys took %.1f%% of traffic; θ=0.99 should concentrate far more",
			top*100)
	}
	t.Logf("hottest 1%% of keys took %.1f%% of traffic", top*100)
}

func TestLowerThetaIsLessSkewed(t *testing.T) {
	t.Parallel()

	const (
		items   = 100_000
		samples = 300_000
	)

	share := func(theta float64) float64 {
		z, err := harness.NewZipfian(items, theta, 42)
		if err != nil {
			t.Fatalf("NewZipfian(%v): %v", theta, err)
		}
		counts := make(map[uint64]int, items)
		for i := 0; i < samples; i++ {
			counts[z.Next()]++
		}
		return topShare(counts, samples, items/100)
	}

	// The skew sweep in Phase 4 (W6) depends on θ actually controlling skew. If it does not, the
	// sweep would show a flat line and be read as "adaptive admission does nothing".
	low, high := share(0.7), share(0.99)
	if low >= high {
		t.Errorf("θ=0.7 concentrated %.1f%% and θ=0.99 concentrated %.1f%%; θ is not controlling skew",
			low*100, high*100)
	}
}

func TestHotKeysAreScatteredAcrossTheIDSpace(t *testing.T) {
	t.Parallel()

	const (
		items   = 100_000
		samples = 300_000
	)
	z, err := harness.NewZipfian(items, 0.99, 42)
	if err != nil {
		t.Fatalf("NewZipfian: %v", err)
	}

	counts := make(map[uint64]int, items)
	for i := 0; i < samples; i++ {
		counts[z.Next()]++
	}

	// An unscrambled Zipfian makes ids 1..N the hottest, so the working set is a contiguous block
	// of rows — physically adjacent in an LSM engine, sharing SST blocks and block-cache pages.
	// That hands MyRocks a locality advantage no real workload has, and it would flatter the
	// uncached baseline every later row is compared against.
	//
	// So the hottest keys must be scattered, not clustered at the low end.
	hottest := topKeys(counts, 100)
	var low int
	var sum uint64
	for _, id := range hottest {
		if id < items/50 { // the bottom 2% of the id space
			low++
		}
		sum += id
	}
	if low > 20 {
		t.Errorf("%d of the 100 hottest keys sit in the bottom 2%% of the id space; "+
			"the distribution is clustered rather than scrambled", low)
	}

	// The mean id of the hot set should sit near the middle of the space if it is genuinely spread.
	mean := float64(sum) / float64(len(hottest))
	if mean < items*0.3 || mean > items*0.7 {
		t.Errorf("the 100 hottest keys average id %.0f in a space of %d; expected roughly central",
			mean, items)
	}
	t.Logf("100 hottest keys: %d in the bottom 2%% of the id space, mean id %.0f", low, mean)
}

// topKeys returns the n most frequently drawn keys.
func topKeys(counts map[uint64]int, n int) []uint64 {
	type kv struct {
		id uint64
		n  int
	}
	all := make([]kv, 0, len(counts))
	for id, c := range counts {
		all = append(all, kv{id, c})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].n > all[j].n })

	out := make([]uint64, 0, n)
	for i := 0; i < n && i < len(all); i++ {
		out = append(out, all[i].id)
	}
	return out
}

func TestUniformGeneratorIsFlat(t *testing.T) {
	t.Parallel()

	const (
		items   = 1000
		samples = 200_000
	)
	u := harness.NewUniform(items, 42)

	counts := make(map[uint64]int, items)
	for i := 0; i < samples; i++ {
		counts[u.Next()]++
	}

	// W1 is the control. It exists to prove the caching effects seen in W2 come from skew rather
	// than from the harness, so it has to be genuinely flat.
	if top := topShare(counts, samples, items/100); top > 0.05 {
		t.Errorf("the hottest 1%% of keys took %.1f%% of a uniform workload", top*100)
	}
}

// topShare returns the fraction of samples taken by the n most frequent keys.
func topShare(counts map[uint64]int, samples, n int) float64 {
	freqs := make([]int, 0, len(counts))
	for _, c := range counts {
		freqs = append(freqs, c)
	}
	// Partial selection is enough: sort descending and take the head.
	for i := 0; i < n && i < len(freqs); i++ {
		maxAt := i
		for j := i + 1; j < len(freqs); j++ {
			if freqs[j] > freqs[maxAt] {
				maxAt = j
			}
		}
		freqs[i], freqs[maxAt] = freqs[maxAt], freqs[i]
	}

	var total int
	for i := 0; i < n && i < len(freqs); i++ {
		total += freqs[i]
	}
	return float64(total) / float64(samples)
}
