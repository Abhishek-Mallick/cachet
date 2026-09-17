package sextant_test

import (
	"math"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/sextant"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// The SLO is the product claim reduced to a number, so what it reports has to be defensible in
// front of someone who does not want to believe it.
//
// Per level, never blended. A single figure would average a level that bypasses the cache entirely
// with one that promises only eventual convergence, and the result would describe no promise Cachet
// actually makes — while hiding the exact trade the levels exist to expose.

func TestAFreshSLOReportsNothingRatherThanPerfection(t *testing.T) {
	t.Parallel()

	// The distinction that keeps the dashboard honest at startup. Zero observations is not 100%
	// consistency; it is no evidence. Reporting a perfect score for a verifier that has not looked
	// at anything yet is the single most misleading thing this component could do.
	s := sextant.NewSLO(time.Minute)

	r := s.Report(consistency.Session, at(0))
	if r.Observations != 0 {
		t.Errorf("Observations = %d on a fresh SLO, want 0", r.Observations)
	}
	if r.Known {
		t.Error("a fresh SLO reported a known consistency figure; no observation has been made")
	}
}

func TestACleanWindowIsOneHundredPercent(t *testing.T) {
	t.Parallel()

	s := sextant.NewSLO(time.Minute)
	for i := 0; i < 100; i++ {
		s.Observe(consistency.Session, at(i%50), false)
	}

	r := s.Report(consistency.Session, at(50))
	if !r.Known {
		t.Fatal("100 observations produced no known figure")
	}
	if r.Consistency != 1 {
		t.Errorf("Consistency = %v with no violations, want 1", r.Consistency)
	}
	if r.Violations != 0 {
		t.Errorf("Violations = %d, want 0", r.Violations)
	}
}

func TestViolationsLowerTheFigure(t *testing.T) {
	t.Parallel()

	s := sextant.NewSLO(time.Minute)
	for i := 0; i < 99; i++ {
		s.Observe(consistency.Session, at(i%50), false)
	}
	s.Observe(consistency.Session, at(50), true)

	r := s.Report(consistency.Session, at(50))
	if r.Observations != 100 {
		t.Fatalf("Observations = %d, want 100", r.Observations)
	}
	if math.Abs(r.Consistency-0.99) > 1e-9 {
		t.Errorf("Consistency = %v, want 0.99", r.Consistency)
	}
}

func TestLevelsAreAccountedSeparately(t *testing.T) {
	t.Parallel()

	// The property that makes the number mean anything. A stale entry that violates EVENTUAL but
	// not SESSION must not drag SESSION's figure down, or the level with the strongest promise
	// would be penalised for the weakest one's failures.
	s := sextant.NewSLO(time.Minute)

	for i := 0; i < 10; i++ {
		s.Observe(consistency.Session, at(i), false)
		s.Observe(consistency.Eventual, at(i), true)
	}

	session := s.Report(consistency.Session, at(10))
	eventual := s.Report(consistency.Eventual, at(10))

	if session.Consistency != 1 {
		t.Errorf("SESSION = %v, want 1; it was charged for EVENTUAL's violations", session.Consistency)
	}
	if eventual.Consistency != 0 {
		t.Errorf("EVENTUAL = %v, want 0", eventual.Consistency)
	}
}

func TestTheWindowRolls(t *testing.T) {
	t.Parallel()

	// A violation from an outage last week must not still be dragging today's number down. An SLO
	// that never forgets is one nobody can ever bring back to green, so nobody tries.
	s := sextant.NewSLO(time.Minute)

	s.Observe(consistency.Session, at(0), true)
	for i := 0; i < 9; i++ {
		s.Observe(consistency.Session, at(1+i), false)
	}

	// Still inside the window: the violation counts.
	within := s.Report(consistency.Session, at(30))
	if within.Violations != 1 {
		t.Errorf("Violations = %d inside the window, want 1", within.Violations)
	}

	// Past it: gone, along with the observations it was measured against.
	after := s.Report(consistency.Session, at(0).Add(2*time.Minute))
	if after.Violations != 0 {
		t.Errorf("Violations = %d after the window rolled, want 0", after.Violations)
	}
}

func TestNinesAreReportedForADashboard(t *testing.T) {
	t.Parallel()

	// "Three nines" is how this gets discussed in a review, so the component computes it rather
	// than leaving every consumer to do it slightly differently.
	s := sextant.NewSLO(time.Minute)
	for i := 0; i < 999; i++ {
		s.Observe(consistency.Session, at(i%50), false)
	}
	s.Observe(consistency.Session, at(50), true)

	r := s.Report(consistency.Session, at(50))
	if got := math.Round(r.Nines*10) / 10; got != 3.0 {
		t.Errorf("Nines = %v for a 0.999 figure, want 3", r.Nines)
	}
}

func TestAPerfectWindowDoesNotReportInfiniteNines(t *testing.T) {
	t.Parallel()

	// log10(1/0) is infinity, which renders as "+Inf" on a dashboard and makes the panel look
	// broken at exactly the moment everything is fine.
	s := sextant.NewSLO(time.Minute)
	for i := 0; i < 100; i++ {
		s.Observe(consistency.Session, at(i%50), false)
	}

	r := s.Report(consistency.Session, at(50))
	if math.IsInf(r.Nines, 0) || math.IsNaN(r.Nines) {
		t.Errorf("Nines = %v on a clean window; a dashboard cannot render that", r.Nines)
	}
}

func TestStrongIsNeverCharged(t *testing.T) {
	t.Parallel()

	// STRONG does not read the cache, so a cache violation cannot be attributed to it. Accepting a
	// STRONG violation here would let a mistake elsewhere quietly corrupt the one number a reader
	// trusts most.
	s := sextant.NewSLO(time.Minute)
	s.Observe(consistency.Strong, at(0), true)

	r := s.Report(consistency.Strong, at(1))
	if r.Violations != 0 {
		t.Errorf("STRONG recorded %d violations; it does not read the cache", r.Violations)
	}
}

func TestSLOIsSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	s := sextant.NewSLO(time.Minute)
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 500; i++ {
				s.Observe(consistency.Session, at(i%50), i%100 == 0)
				s.Report(consistency.Session, at(50))
			}
		}()
	}
	for g := 0; g < 8; g++ {
		<-done
	}

	if r := s.Report(consistency.Session, at(50)); r.Observations != 4000 {
		t.Errorf("Observations = %d after 4000 concurrent observations, want 4000", r.Observations)
	}
}
