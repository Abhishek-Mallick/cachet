package faults_test

import (
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/faults"
)

func sample() []faults.Record {
	return []faults.Record{{
		Number:      1,
		Title:       "Cache node unreachable mid-traffic",
		Injection:   "Toxiproxy: cache proxy disabled",
		Claim:       "Reads continue, served from the database.",
		Fired:       "`cache_operations_total{result=\"error\"}` = 3",
		Observed:    "Every read succeeded with the pre-fault payload.",
		Explanation: "`cachetctl health` reports the node as failing.",
	}}
}

func TestEachRecordedFaultBecomesASection(t *testing.T) {
	t.Parallel()

	got := faults.Render(sample())
	for _, want := range []string{
		"Cache node unreachable mid-traffic",
		"Toxiproxy: cache proxy disabled",
		"cachetctl health",
		"Every read succeeded",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered document is missing %q:\n%s", want, got)
		}
	}
}

// The evidence that an injection fired is the difference between a fault record and a wish. A
// serene green row for a toxic that never applied asserts nothing at all.
func TestTheEvidenceThatTheInjectionFiredIsRendered(t *testing.T) {
	t.Parallel()

	got := faults.Render(sample())
	if !strings.Contains(got, `cache_operations_total{result="error"}`) {
		t.Errorf("the proof the injection fired is not in the document:\n%s", got)
	}
}

// The document must show what is NOT covered. A file listing four passing faults, with no
// indication that nine were planned, reads as completeness.
func TestUncoveredFaultsAreListedAsOutstanding(t *testing.T) {
	t.Parallel()

	got := faults.Render(sample())
	if !strings.Contains(got, "Not yet covered") {
		t.Fatalf("no outstanding section:\n%s", got)
	}
	// Fault 9 is in the catalogue and absent from the sample, so it must appear as outstanding.
	idx := strings.Index(got, "Not yet covered")
	if !strings.Contains(got[idx:], faults.Catalogue[9]) {
		t.Errorf("fault 9 (%q) is uncovered but not listed as outstanding:\n%s", faults.Catalogue[9], got[idx:])
	}
	// ...and fault 1 is covered, so it must not.
	if strings.Contains(got[idx:], faults.Catalogue[1]) {
		t.Errorf("fault 1 is covered but listed as outstanding:\n%s", got[idx:])
	}
}

func TestCoverageIsStatedAsAFraction(t *testing.T) {
	t.Parallel()

	got := faults.Render(sample())
	if !strings.Contains(got, "1 of 9") {
		t.Errorf("coverage is not stated plainly:\n%s", got)
	}
}

// An empty run must not render a document that looks like a pass.
func TestNoRecordsRendersNoClaimOfCoverage(t *testing.T) {
	t.Parallel()

	got := faults.Render(nil)
	if strings.Contains(got, "✅") {
		t.Errorf("an empty run rendered a tick:\n%s", got)
	}
	if !strings.Contains(got, "0 of 9") {
		t.Errorf("an empty run did not say so plainly:\n%s", got)
	}
}

// The chaos suite runs under `-count=2`, so every fault records itself twice. Left alone that
// rendered "Coverage: 18 of 9" — a number that is not merely wrong but impossible, in a document
// whose entire job is to be checkable.
func TestARepeatedRunDoesNotInflateCoverage(t *testing.T) {
	t.Parallel()

	rec := sample()[0]
	second := rec
	second.Fired = "a second run, with different counts"

	got := faults.Render([]faults.Record{rec, second})

	if !strings.Contains(got, "1 of 9") {
		t.Errorf("two records of the same fault were counted as two:\n%s", got)
	}
	if strings.Count(got, "## 1. "+rec.Title) != 1 {
		t.Errorf("the fault was rendered twice:\n%s", got)
	}
	// The newest observation wins: a rerun is a fresher measurement, not a duplicate to discard.
	if !strings.Contains(got, "a second run, with different counts") {
		t.Errorf("the later record was dropped in favour of the earlier one:\n%s", got)
	}
}
