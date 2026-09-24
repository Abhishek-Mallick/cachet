package sextant_test

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
	"github.com/Abhishek-Mallick/cachet/pkg/sextant"
)

// A tier is a statement about what could be measured, and it has to survive all the way onto the
// dashboard. These tests are about the one failure mode that would make Sextant worse than not
// existing: a run that could only compare values being read as a claim about BOUNDED.

// TestAValueTierObservationCannotViolateAnOrderedLevel. SESSION and BOUNDED are statements about
// WHICH database state an entry holds. Two values differing says an entry is not current; it does
// not say what it is.
func TestAValueTierObservationCannotViolateAnOrderedLevel(t *testing.T) {
	t.Parallel()

	bound := sextant.NewPropagationBound(50*time.Millisecond, time.Second, 100*time.Millisecond)
	now := time.Now()

	obs := sextant.Observation{
		Key:            "k",
		Shard:          "shard0",
		Diff:           sextant.ValueDifference(false),
		BehindSince:    now.Add(-time.Hour),
		Now:            now,
		Watermark:      99,
		WatermarkKnown: true,
		BoundedWindow:  time.Millisecond,
	}

	v, isViolation := sextant.Classify(obs, bound)
	if !isViolation {
		t.Fatal("an entry an hour behind was not a violation")
	}
	if !v.Violates(consistency.Eventual) {
		t.Error("EVENTUAL is the one level a value comparison CAN evaluate, and it was not reported")
	}
	for _, level := range []consistency.Level{consistency.Session, consistency.Bounded} {
		if v.Violates(level) {
			t.Errorf("%s was reported from a comparison with no versions in it; that is a claim "+
				"the observation cannot support", level)
		}
	}
	// And the rendering must not invent version numbers it does not have.
	if strings.Contains(v.String(), "fv=") {
		t.Errorf("the violation renders versions it never observed: %s", v)
	}
}

// TestAVersionTierObservationEvaluatesEveryLevel is the control: the same entry, with versions,
// violates what the model says it should.
func TestAVersionTierObservationEvaluatesEveryLevel(t *testing.T) {
	t.Parallel()

	bound := sextant.NewPropagationBound(50*time.Millisecond, time.Second, 100*time.Millisecond)
	now := time.Now()

	v, isViolation := sextant.Classify(sextant.Observation{
		Key:            "k",
		Shard:          "shard0",
		Diff:           sextant.VersionDifference(10, 20),
		BehindSince:    now.Add(-time.Hour),
		Now:            now,
		Watermark:      15,
		WatermarkKnown: true,
		BoundedWindow:  time.Millisecond,
	}, bound)
	if !isViolation {
		t.Fatal("an entry an hour behind was not a violation")
	}
	for _, level := range []consistency.Level{consistency.Eventual, consistency.Session, consistency.Bounded} {
		if !v.Violates(level) {
			t.Errorf("%s was not reported for an entry an hour behind its watermark", level)
		}
	}
}

// TestTheTierIsOnEveryExportedSeries. A dashboard that could not tell which tier produced a number
// would let a value-tier run be read as a version-tier claim.
func TestTheTierIsOnEveryExportedSeries(t *testing.T) {
	t.Parallel()

	for _, tier := range []sextant.Tier{sextant.TierValue, sextant.TierVersion, sextant.TierCachet} {
		t.Run(string(tier), func(t *testing.T) {
			t.Parallel()

			slo := sextant.NewSLO(time.Minute)
			v, err := sextant.NewVerifier(sextant.VerifierOptions{
				Cache: &fakeCache{}, Origin: &fakeOrigin{}, Keys: sextant.NewRecentKeys(4),
				SLO: slo, Tier: tier,
			})
			if err != nil {
				t.Fatalf("NewVerifier: %v", err)
			}

			reg := prometheus.NewRegistry()
			if err := sextant.RegisterMetrics(reg, slo, v); err != nil {
				t.Fatalf("RegisterMetrics: %v", err)
			}
			families, err := reg.Gather()
			if err != nil {
				t.Fatalf("Gather: %v", err)
			}
			if len(families) == 0 {
				t.Fatal("nothing was exported")
			}

			levels := map[string]bool{}
			for _, fam := range families {
				for _, m := range fam.GetMetric() {
					if !hasLabel(m, "tier", string(tier)) {
						t.Errorf("%s has no tier label", fam.GetName())
					}
					for _, l := range m.GetLabel() {
						if l.GetName() == "level" {
							levels[l.GetValue()] = true
						}
					}
				}
			}

			// A tier that cannot order an entry against the database must not export SESSION or
			// BOUNDED at all: exporting them as zero violations is a clean bill of health nobody
			// measured.
			if !tier.ComparesVersions() {
				for _, absent := range []string{"SESSION", "BOUNDED"} {
					if levels[absent] {
						t.Errorf("%s exported a %s series it cannot evaluate", tier, absent)
					}
				}
			}
			if !levels["EVENTUAL"] {
				t.Errorf("%s exported no EVENTUAL series", tier)
			}
		})
	}
}

func hasLabel(m *dto.Metric, name, value string) bool {
	for _, l := range m.GetLabel() {
		if l.GetName() == name && l.GetValue() == value {
			return true
		}
	}
	return false
}
