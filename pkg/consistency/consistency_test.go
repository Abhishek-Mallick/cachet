package consistency_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

func TestLevelNamesRoundTrip(t *testing.T) {
	t.Parallel()

	for _, lv := range []consistency.Level{
		consistency.Strong, consistency.Session, consistency.Bounded, consistency.Eventual,
	} {
		got, err := consistency.ParseLevel(lv.String())
		if err != nil {
			t.Errorf("ParseLevel(%q): %v", lv.String(), err)
			continue
		}
		if got != lv {
			t.Errorf("ParseLevel(%q) = %v, want %v", lv.String(), got, lv)
		}
	}
}

func TestParseLevelRejectsUnknownNames(t *testing.T) {
	t.Parallel()

	// A typo in a config file must fail at boot, not silently select a weaker guarantee than the
	// operator asked for (CONTRIBUTING.md rule 15).
	if _, err := consistency.ParseLevel("stronk"); err == nil {
		t.Error("ParseLevel(\"stronk\") succeeded; unknown levels must be rejected")
	}
}

func TestParseLevelIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	got, err := consistency.ParseLevel("SESSION")
	if err != nil {
		t.Fatalf("ParseLevel: %v", err)
	}
	if got != consistency.Session {
		t.Errorf("ParseLevel(\"SESSION\") = %v, want Session", got)
	}
}

func TestBoundedRequiresAStalenessBound(t *testing.T) {
	t.Parallel()

	req := consistency.Requirement{Level: consistency.Bounded}
	if err := req.Validate(); !errors.Is(err, consistency.ErrMissingStalenessBound) {
		t.Errorf("Validate() = %v, want ErrMissingStalenessBound", err)
	}
}

func TestNonBoundedRejectsAStalenessBound(t *testing.T) {
	t.Parallel()

	// Accepting and ignoring the field would let a caller believe they had asked for a freshness
	// guarantee they are not getting. Silence about a downgraded guarantee is the failure mode this
	// whole project exists to remove.
	req := consistency.Requirement{Level: consistency.Session, StalenessBound: time.Second}
	if err := req.Validate(); !errors.Is(err, consistency.ErrUnexpectedStalenessBound) {
		t.Errorf("Validate() = %v, want ErrUnexpectedStalenessBound", err)
	}
}

func TestBoundedRejectsANegativeBound(t *testing.T) {
	t.Parallel()

	req := consistency.Requirement{Level: consistency.Bounded, StalenessBound: -time.Second}
	if err := req.Validate(); err == nil {
		t.Error("Validate() accepted a negative staleness bound")
	}
}

func TestValidBoundedRequirementPasses(t *testing.T) {
	t.Parallel()

	req := consistency.Requirement{Level: consistency.Bounded, StalenessBound: 5 * time.Second}
	if err := req.Validate(); err != nil {
		t.Errorf("Validate(): %v", err)
	}
}

func TestStrongReadsBypassTheCache(t *testing.T) {
	t.Parallel()

	if !consistency.Strong.BypassesCache() {
		t.Error("Strong.BypassesCache() = false; STRONG must never consult the cache")
	}
	for _, lv := range []consistency.Level{consistency.Session, consistency.Bounded, consistency.Eventual} {
		if lv.BypassesCache() {
			t.Errorf("%v.BypassesCache() = true, want false", lv)
		}
	}
}

func TestOnlyStrongSessionAndBoundedRequireTheWatermark(t *testing.T) {
	t.Parallel()

	// EVENTUAL is the control group: it deliberately declines read-own-writes so that the cost of
	// the other levels is visible in the benchmark table.
	if consistency.Eventual.RequiresWatermark() {
		t.Error("Eventual.RequiresWatermark() = true; EVENTUAL does not offer read-own-writes")
	}
	for _, lv := range []consistency.Level{consistency.Session, consistency.Bounded} {
		if !lv.RequiresWatermark() {
			t.Errorf("%v.RequiresWatermark() = false, want true", lv)
		}
	}
}

func TestSessionKeepsTheHighestVersionPerShard(t *testing.T) {
	t.Parallel()

	s := consistency.NewToken(8)
	s.Advance("shard0", 100)
	s.Advance("shard0", 50) // an out-of-order ack, or a retry of an older write

	v, known := s.Watermark("shard0")
	if !known {
		t.Fatal("Watermark(\"shard0\") reported unknown after Advance")
	}
	// A watermark that could move backwards would let a session read a value older than one it has
	// already seen, breaking monotonic reads.
	if v != 100 {
		t.Errorf("Watermark = %d, want 100 — the watermark must never regress", v)
	}
}

func TestUntouchedShardHasNoWatermarkAndIsNotDegraded(t *testing.T) {
	t.Parallel()

	s := consistency.NewToken(8)

	if _, known := s.Watermark("shard3"); known {
		t.Error("Watermark on an untouched shard reported a value")
	}
	// An untouched shard is not a lost guarantee: a session that has written nothing there has no
	// writes to read. That is correct behaviour, not a degradation.
	if s.Degraded("shard3") {
		t.Error("Degraded(\"shard3\") = true for a shard this session never touched")
	}
}

func TestSessionEvictsTheOldestWatermarkPastTheCap(t *testing.T) {
	t.Parallel()

	s := consistency.NewToken(2)
	s.Advance("shard0", 10)
	s.Advance("shard1", 20)
	s.Advance("shard2", 30) // exceeds the cap

	if _, known := s.Watermark("shard0"); known {
		t.Error("shard0's watermark survived past the cap; the oldest must be evicted")
	}
	for _, id := range []string{"shard1", "shard2"} {
		if _, known := s.Watermark(id); !known {
			t.Errorf("%s's watermark was evicted although it is newer", id)
		}
	}
}

func TestAnEvictedShardIsReportedAsDegraded(t *testing.T) {
	t.Parallel()

	s := consistency.NewToken(2)
	s.Advance("shard0", 10)
	s.Advance("shard1", 20)
	s.Advance("shard2", 30)

	// This is the distinction that makes eviction honest. An evicted shard is NOT the same as an
	// untouched one: the session did write there, and the guarantee has been silently lost unless
	// the response says so (CONSISTENCY.md §4).
	if !s.Degraded("shard0") {
		t.Error("Degraded(\"shard0\") = false after its watermark was evicted")
	}
	if s.Degraded("shard1") {
		t.Error("Degraded(\"shard1\") = true although its watermark is intact")
	}
}

func TestRefreshingAWatermarkMakesItTheNewest(t *testing.T) {
	t.Parallel()

	s := consistency.NewToken(2)
	s.Advance("shard0", 10)
	s.Advance("shard1", 20)
	s.Advance("shard0", 30) // shard0 is used again, so shard1 is now the oldest
	s.Advance("shard2", 40)

	if _, known := s.Watermark("shard0"); !known {
		t.Error("shard0 was evicted although it was the most recently used")
	}
	if _, known := s.Watermark("shard1"); known {
		t.Error("shard1 survived although it was the least recently used")
	}
}

func TestMergeTakesTheHighestVersionPerShard(t *testing.T) {
	t.Parallel()

	// This is causal propagation across a service hop: if A writes and C reads, C needs A's
	// watermark, and merging must never lose the higher of the two (CONSISTENCY.md §3.2).
	s := consistency.NewToken(8)
	s.Advance("shard0", 100)
	s.Advance("shard1", 5)

	s.Merge(map[string]uint64{"shard0": 50, "shard1": 500, "shard2": 7})

	for _, tc := range []struct {
		shard string
		want  uint64
	}{{"shard0", 100}, {"shard1", 500}, {"shard2", 7}} {
		got, known := s.Watermark(tc.shard)
		if !known {
			t.Errorf("%s missing after Merge", tc.shard)
			continue
		}
		if got != tc.want {
			t.Errorf("%s = %d after Merge, want %d", tc.shard, got, tc.want)
		}
	}
}

func TestWatermarksReturnsACopy(t *testing.T) {
	t.Parallel()

	s := consistency.NewToken(8)
	s.Advance("shard0", 100)

	snapshot := s.Watermarks()
	snapshot["shard0"] = 1 // a caller mutating what it was handed

	if v, _ := s.Watermark("shard0"); v != 100 {
		t.Errorf("Watermark = %d after a caller mutated the returned map; want 100", v)
	}
}

func TestSessionIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	// The SDK shares one session across an application's goroutines, so this is the normal case
	// rather than an edge case.
	s := consistency.NewToken(16)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				s.Advance("shard0", uint64(i))
				s.Watermark("shard0")
				s.Watermarks()
			}
		}(g)
	}
	wg.Wait()

	if v, _ := s.Watermark("shard0"); v != 199 {
		t.Errorf("Watermark = %d after concurrent advances, want 199", v)
	}
}

func TestNewTokenRejectsANonPositiveCap(t *testing.T) {
	t.Parallel()

	// A zero cap would evict every watermark immediately and silently disable read-own-writes.
	s := consistency.NewToken(0)
	s.Advance("shard0", 10)
	if _, known := s.Watermark("shard0"); !known {
		t.Error("a non-positive cap disabled watermarking instead of falling back to the default")
	}
}
