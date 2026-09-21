package harness_test

import (
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/bench/harness"
)

const sampleOutput = `goos: linux
goarch: amd64
pkg: github.com/Abhishek-Mallick/cachet/internal/hashring
BenchmarkRingLookup-4   	38875171	        31.12 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	github.com/Abhishek-Mallick/cachet/internal/hashring	1.500s
pkg: github.com/Abhishek-Mallick/cachet/pkg/consistency
BenchmarkTokenProto-4   	 6369271	       187.5 ns/op	     256 B/op	       2 allocs/op
PASS
`

func TestParsesBenchmarkOutput(t *testing.T) {
	t.Parallel()

	got, err := harness.ParseBenchmarks(strings.NewReader(sampleOutput))
	if err != nil {
		t.Fatalf("ParseBenchmarks: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("parsed %d benchmarks, want 2", len(got))
	}

	// The -N suffix is the GOMAXPROCS the runner happened to have. Keeping it would make the same
	// benchmark a different column on a 4-core runner than on an 8-core one, and the table would
	// grow a column every time GitHub changed its fleet.
	if got[0].Name != "BenchmarkRingLookup" {
		t.Errorf("Name = %q, want the -N suffix stripped", got[0].Name)
	}
	if got[0].NsPerOp != 31.12 {
		t.Errorf("NsPerOp = %v, want 31.12", got[0].NsPerOp)
	}
	if got[1].AllocsPerOp != 2 || got[1].BytesPerOp != 256 {
		t.Errorf("allocs/bytes = %d/%d, want 2/256", got[1].AllocsPerOp, got[1].BytesPerOp)
	}
}

func TestABenchmarkWithoutMemStatsStillParses(t *testing.T) {
	t.Parallel()

	// -benchmem is easy to forget. Dropping the row entirely would silently shrink the table.
	out := "BenchmarkThing-8   	1000000	      1234 ns/op\n"
	got, err := harness.ParseBenchmarks(strings.NewReader(out))
	if err != nil {
		t.Fatalf("ParseBenchmarks: %v", err)
	}
	if len(got) != 1 || got[0].NsPerOp != 1234 {
		t.Fatalf("got %+v, want one benchmark at 1234 ns/op", got)
	}
}

func TestNonBenchmarkLinesAreIgnored(t *testing.T) {
	t.Parallel()

	out := "goos: linux\nPASS\nok  \tsome/pkg\t1.5s\nFAIL\n"
	got, err := harness.ParseBenchmarks(strings.NewReader(out))
	if err != nil {
		t.Fatalf("ParseBenchmarks: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("parsed %d benchmarks from output containing none", len(got))
	}
}

func TestATableIsCreatedWhenAbsent(t *testing.T) {
	t.Parallel()

	got := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "abc1234", Date: "2026-09-18",
		Results: []harness.BenchResult{
			{Name: "BenchmarkRingLookup", NsPerOp: 31.12, AllocsPerOp: 0},
		},
	}, 10)

	// Column headings drop the "Benchmark" prefix: every column would otherwise begin with the same
	// nine characters, which costs width and adds nothing. The round trip re-adds it, so a table
	// re-read by the next commit still matches its results by full name.
	for _, want := range []string{"abc1234", "RingLookup", "31.1"} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered table missing %q:\n%s", want, got)
		}
	}
}

func TestNewestRowIsFirst(t *testing.T) {
	t.Parallel()

	// Read during a regression hunt, when the question is always "what changed most recently".
	first := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "old1111", Date: "2026-09-17",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 10}},
	}, 10)
	second := harness.RenderBenchTable([]byte(first), harness.BenchRow{
		Commit: "new2222", Date: "2026-09-18",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 20}},
	}, 10)

	newIdx := strings.Index(second, "new2222")
	oldIdx := strings.Index(second, "old1111")
	if newIdx < 0 || oldIdx < 0 {
		t.Fatalf("both commits should appear:\n%s", second)
	}
	if newIdx > oldIdx {
		t.Error("the newest row is not first")
	}
}

func TestHistoryIsTrimmed(t *testing.T) {
	t.Parallel()

	// An append-only file grows until the diff on every commit is unreadable, at which point people
	// stop reading it — which is the same as not having it.
	var table string
	for i := 0; i < 20; i++ {
		table = harness.RenderBenchTable([]byte(table), harness.BenchRow{
			Commit:  "commit" + string(rune('a'+i)),
			Date:    "2026-09-18",
			Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: float64(i)}},
		}, 5)
	}

	rows := 0
	for _, line := range strings.Split(table, "\n") {
		if strings.HasPrefix(line, "| `") {
			rows++
		}
	}
	if rows != 5 {
		t.Errorf("table holds %d rows, want the 5 most recent", rows)
	}
}

func TestANewBenchmarkBecomesANewColumn(t *testing.T) {
	t.Parallel()

	first := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 10}},
	}, 10)
	second := harness.RenderBenchTable([]byte(first), harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 11},
			{Name: "BenchmarkB", NsPerOp: 22},
		},
	}, 10)

	if !strings.Contains(second, "| B<br/>") {
		t.Errorf("a newly added benchmark did not become a column:\n%s", second)
	}
	// The older row has no value for the new column, which must render as a gap rather than a zero.
	// A zero would read as "this got infinitely fast" on a regression graph.
	if !strings.Contains(second, "—") {
		t.Errorf("a missing measurement did not render as a gap:\n%s", second)
	}
}

func TestTheHeaderExplainsWhatTheNumbersAreNot(t *testing.T) {
	t.Parallel()

	// The single most important thing in the file. These run on a shared CI runner, which is exactly
	// the environment this project says produces untrustworthy latency figures. Without the caveat
	// in the file itself, someone will eventually quote a ns/op from here as a published benchmark.
	got := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "abc1234", Date: "2026-09-18",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 1}},
	}, 10)

	for _, want := range []string{"regression", "not", "allocs"} {
		if !strings.Contains(strings.ToLower(got), want) {
			t.Errorf("the header does not mention %q:\n%s", want, got)
		}
	}
}

func TestTheHeaderWarnsAgainstLocallyGeneratedRows(t *testing.T) {
	t.Parallel()

	// A row generated by running benchctl on a laptop lands in the same table as the CI rows and is
	// indistinguishable from them, so it reads as a regression when the only thing that changed was
	// the machine. This happened once already: a hand-seeded row measured on a developer machine put
	// the controller at 414ns against CI's 1.20µs, which looks like a 3x win and is not one.
	//
	// "machine" alone is not enough to assert on — the allocs/op paragraph already says it.
	got := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "abc1234", Date: "2026-09-18",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 1}},
	}, 10)

	if !strings.Contains(strings.ToLower(got), "comparable") {
		t.Errorf("the header does not say rows are comparable only to each other:\n%s", got)
	}
}

func TestASparklineSpansTheRangeOfItsValues(t *testing.T) {
	t.Parallel()

	got := harness.Sparkline([]float64{1, 2, 3, 4, 5, 6, 7, 8})
	if len([]rune(got)) != 8 {
		t.Fatalf("Sparkline produced %d runes for 8 values: %q", len([]rune(got)), got)
	}
	runes := []rune(got)
	if runes[0] == runes[len(runes)-1] {
		t.Errorf("a rising series rendered flat: %q", got)
	}
	// The smallest value must be the shortest bar and the largest the tallest, or the picture is
	// telling a different story from the numbers.
	if runes[0] > runes[len(runes)-1] {
		t.Errorf("a rising series rendered as falling: %q", got)
	}
}

func TestAFlatSeriesRendersFlat(t *testing.T) {
	t.Parallel()

	// Identical values must not be scaled into a dramatic shape by normalisation. A benchmark that
	// did not move is the single most common case, and it must look like nothing happened.
	got := harness.Sparkline([]float64{50, 50, 50, 50})
	for _, r := range got {
		if r != []rune(got)[0] {
			t.Fatalf("a flat series rendered with variation: %q", got)
		}
	}
}

func TestTheTableCarriesATrendColumn(t *testing.T) {
	t.Parallel()

	first := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 100, AllocsPerOp: 1}},
	}, 10)
	second := harness.RenderBenchTable([]byte(first), harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 200, AllocsPerOp: 1}},
	}, 10)

	if !strings.Contains(second, "Trend") {
		t.Fatalf("no trend column:\n%s", second)
	}
	// A doubling is a regression and must read as one, with a sign a reader cannot misinterpret.
	if !strings.Contains(second, "+100") {
		t.Errorf("a 2x regression is not shown as +100%%:\n%s", second)
	}
}

// The trend column must not be mistaken for a benchmark when the next commit reads the file back.
func TestTheTrendColumnSurvivesARoundTrip(t *testing.T) {
	t.Parallel()

	first := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 100, AllocsPerOp: 1}},
	}, 10)
	second := harness.RenderBenchTable([]byte(first), harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 120, AllocsPerOp: 1}},
	}, 10)
	third := harness.RenderBenchTable([]byte(second), harness.BenchRow{
		Commit: "ccc3333", Date: "2026-09-19",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 130, AllocsPerOp: 1}},
	}, 10)

	// Three commits, one real benchmark: the header must be Commit, Date, A, Trend — and stay that
	// way however many times the file is read back and rewritten. A trend value parsed as a
	// measurement would add a column per round trip.
	header := ""
	for _, line := range strings.Split(third, "\n") {
		if strings.HasPrefix(line, "| Commit |") {
			header = line
		}
	}
	if header == "" {
		t.Fatalf("no header row:\n%s", third)
	}
	if cols := strings.Count(header, "|") - 1; cols != 4 {
		t.Errorf("header has %d columns, want 4 (Commit, Date, A, Trend): %s", cols, header)
	}
	if strings.Contains(third, "Trend<br/><sub>ns/op vs previous</sub><br/>") {
		t.Errorf("the trend column was re-wrapped as a benchmark:\n%s", third)
	}
	if !strings.Contains(third, "aaa1111") {
		t.Errorf("history was lost across the round trip:\n%s", third)
	}
}

// The oldest row has nothing to compare against. A 0% there would claim "no change" for a
// measurement that was never made.
func TestTheOldestRowHasNoTrend(t *testing.T) {
	t.Parallel()

	got := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 100, AllocsPerOp: 1}},
	}, 10)
	if strings.Contains(got, "0%") {
		t.Errorf("the first commit was given a 0%% trend against nothing:\n%s", got)
	}
}

func TestPerBenchmarkSparklinesAreRendered(t *testing.T) {
	t.Parallel()

	first := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{{Name: "BenchmarkRingLookup", NsPerOp: 100, AllocsPerOp: 0}},
	}, 10)
	second := harness.RenderBenchTable([]byte(first), harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{{Name: "BenchmarkRingLookup", NsPerOp: 150, AllocsPerOp: 0}},
	}, 10)

	if !strings.Contains(second, "RingLookup") {
		t.Fatalf("no per-benchmark trend section:\n%s", second)
	}
	// Oldest to newest, so the picture reads left to right like every other chart.
	idx := strings.Index(second, "## Trend")
	if idx < 0 {
		t.Fatalf("no trend section heading:\n%s", second)
	}
	if !strings.Contains(second[idx:], "100.0ns → 150.0ns") {
		t.Errorf("the trend section does not state the endpoints oldest-first:\n%s", second[idx:])
	}
}

// A generator change must be able to re-render the committed file without inventing a measurement.
// Re-rendering is not the same as recording: no row is added, and no existing row moves.
func TestRerenderAddsNoRow(t *testing.T) {
	t.Parallel()

	first := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 100, AllocsPerOp: 1}},
	}, 10)
	second := harness.RenderBenchTable([]byte(first), harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 150, AllocsPerOp: 1}},
	}, 10)

	again := harness.RerenderBenchTable([]byte(second), 10)

	countRows := func(s string) int {
		n := 0
		for _, line := range strings.Split(s, "\n") {
			if strings.HasPrefix(line, "| `") {
				n++
			}
		}
		return n
	}
	if got, want := countRows(again), countRows(second); got != want {
		t.Errorf("re-render changed the row count: %d, want %d", got, want)
	}
	for _, commit := range []string{"aaa1111", "bbb2222"} {
		if !strings.Contains(again, commit) {
			t.Errorf("re-render lost %s:\n%s", commit, again)
		}
	}
	// Idempotent: rendering the result again must not drift.
	if third := harness.RerenderBenchTable([]byte(again), 10); third != again {
		t.Error("re-render is not idempotent; the file would change on every run")
	}
}

// With only two commits a sparkline always spans its full range: a 1% move and a 10x move draw the
// identical shape. The magnitude has to be written next to it or the picture overstates the story.
func TestTheTrendSectionStatesTheMagnitude(t *testing.T) {
	t.Parallel()

	first := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 100, AllocsPerOp: 0}},
	}, 10)
	second := harness.RenderBenchTable([]byte(first), harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{{Name: "BenchmarkA", NsPerOp: 101, AllocsPerOp: 0}},
	}, 10)

	idx := strings.Index(second, "## Trend")
	if idx < 0 {
		t.Fatalf("no trend section:\n%s", second)
	}
	if !strings.Contains(second[idx:], "+1%") {
		t.Errorf("a 1%% move is drawn as a full-range sparkline with no magnitude beside it:\n%s", second[idx:])
	}
}

// A commit that touches nothing on the request path cannot make every benchmark faster at once.
//
// This happened on 2026-09-18: commit 16bbd2d changed a Makefile, a README, a Dockerfile and a Helm
// chart, and every one of five unrelated benchmarks came back 23–25% faster with zero change in
// allocations. That is a quieter runner, and the trend column reported it as a 25% improvement.
//
// Uniform direction across unrelated benchmarks, similar magnitude, and flat allocation counts is
// mechanically detectable, so it gets labelled rather than celebrated.
func TestAUniformShiftIsLabelledAsAHostEffect(t *testing.T) {
	t.Parallel()

	prev := harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 1000, AllocsPerOp: 3},
			{Name: "BenchmarkB", NsPerOp: 40, AllocsPerOp: 0},
			{Name: "BenchmarkC", NsPerOp: 200, AllocsPerOp: 2},
		},
	}
	// Everything ~25% faster, allocations untouched.
	cur := harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 750, AllocsPerOp: 3},
			{Name: "BenchmarkB", NsPerOp: 30, AllocsPerOp: 0},
			{Name: "BenchmarkC", NsPerOp: 152, AllocsPerOp: 2},
		},
	}

	got := harness.RenderBenchTable([]byte(harness.RenderBenchTable(nil, prev, 10)), cur, 10)
	if !strings.Contains(got, "· host?") {
		t.Errorf("a uniform 25%% shift with flat allocations was not labelled a host effect:\n%s", got)
	}
}

// A real change does not move everything by the same amount, and usually moves allocations.
func TestARealChangeIsNotLabelledAsAHostEffect(t *testing.T) {
	t.Parallel()

	prev := harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 1000, AllocsPerOp: 3},
			{Name: "BenchmarkB", NsPerOp: 40, AllocsPerOp: 0},
			{Name: "BenchmarkC", NsPerOp: 200, AllocsPerOp: 2},
		},
	}
	// One benchmark improves sharply; the others barely move. This is what optimising one thing
	// looks like, and it must not be explained away as the runner being quiet.
	cur := harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 250, AllocsPerOp: 3},
			{Name: "BenchmarkB", NsPerOp: 40, AllocsPerOp: 0},
			{Name: "BenchmarkC", NsPerOp: 198, AllocsPerOp: 2},
		},
	}

	got := harness.RenderBenchTable([]byte(harness.RenderBenchTable(nil, prev, 10)), cur, 10)
	if strings.Contains(got, "· host?") {
		t.Errorf("a genuine single-benchmark improvement was dismissed as a host effect:\n%s", got)
	}
}

// Allocations are deterministic: if they moved, the code moved, whatever the runner was doing.
func TestAnAllocationChangeIsNeverAHostEffect(t *testing.T) {
	t.Parallel()

	prev := harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 1000, AllocsPerOp: 3},
			{Name: "BenchmarkB", NsPerOp: 40, AllocsPerOp: 1},
			{Name: "BenchmarkC", NsPerOp: 200, AllocsPerOp: 2},
		},
	}
	cur := harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 750, AllocsPerOp: 3},
			{Name: "BenchmarkB", NsPerOp: 30, AllocsPerOp: 0}, // one allocation gone
			{Name: "BenchmarkC", NsPerOp: 152, AllocsPerOp: 2},
		},
	}

	got := harness.RenderBenchTable([]byte(harness.RenderBenchTable(nil, prev, 10)), cur, 10)
	if strings.Contains(got, "· host?") {
		t.Errorf("allocations changed, so the code changed; this is not a host effect:\n%s", got)
	}
}

// The uniform shift that prompted this was visible end-to-end across the history rather than
// between two adjacent commits: five unrelated benchmarks each ~23% faster from the oldest row to
// the newest, with allocations flat throughout. Five lines all reading -2x% invite the conclusion
// that the code got faster, so the section says what that pattern usually means.
func TestTheTrendSectionNotesAnEndToEndUniformShift(t *testing.T) {
	t.Parallel()

	table := harness.RenderBenchTable(nil, harness.BenchRow{
		Commit: "aaa1111", Date: "2026-09-17",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 1000, AllocsPerOp: 3},
			{Name: "BenchmarkB", NsPerOp: 40, AllocsPerOp: 0},
			{Name: "BenchmarkC", NsPerOp: 200, AllocsPerOp: 2},
		},
	}, 10)
	table = harness.RenderBenchTable([]byte(table), harness.BenchRow{
		Commit: "bbb2222", Date: "2026-09-18",
		Results: []harness.BenchResult{
			{Name: "BenchmarkA", NsPerOp: 770, AllocsPerOp: 3},
			{Name: "BenchmarkB", NsPerOp: 30, AllocsPerOp: 0},
			{Name: "BenchmarkC", NsPerOp: 154, AllocsPerOp: 2},
		},
	}, 10)

	idx := strings.Index(table, "## Trend")
	if idx < 0 {
		t.Fatal("no trend section")
	}
	if !strings.Contains(strings.ToLower(table[idx:]), "machine") {
		t.Errorf("every benchmark moved the same way by a similar amount and the section does not "+
			"say what that usually means:\n%s", table[idx:])
	}
}
