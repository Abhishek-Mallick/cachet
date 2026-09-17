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
