package sextant_test

import (
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/sextant"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// This file is the definition in CONSISTENCY.md §7, executable:
//
//	A violation is a cache entry e for key k on shard s such that e.fv < db_version(k) AND the
//	entry has existed in that state for longer than the propagation bound P.
//
// The second clause is the whole difficulty, and the reason this is a verifier rather than a
// monitor. An entry that is momentarily behind while an invalidation is in flight is NOT a
// violation — the model never promised instantaneous propagation to other sessions. An entry still
// behind after P is a violation, because something was dropped, reordered or lost.
//
// Getting that line wrong in either direction destroys the product. Too tight and every healthy
// system reports violations, the number becomes noise, and nobody looks at the dashboard again. Too
// loose and real staleness is reported as healthy, which is worse than having no verifier at all
// because it comes with a reassuring green number.

func bound() sextant.PropagationBound {
	// P = write_path_invalidation_budget + cdc_lag_bound + max_clock_skew, per §7. Measured from
	// configuration rather than guessed, so the number on the dashboard is derived from what the
	// engine was actually promising.
	return sextant.NewPropagationBound(50*time.Millisecond, 5*time.Second, 250*time.Millisecond)
}

func TestThePropagationBoundIsTheSumOfItsParts(t *testing.T) {
	t.Parallel()

	p := bound()
	want := 50*time.Millisecond + 5*time.Second + 250*time.Millisecond
	if p.Duration() != want {
		t.Errorf("Duration() = %s, want %s", p.Duration(), want)
	}
}

func TestAnUpToDateEntryIsNotAViolation(t *testing.T) {
	t.Parallel()

	obs := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 100, FillVersion: 100,
		BehindSince: at(0), Now: at(30),
	}
	if v, is := sextant.Classify(obs, bound()); is {
		t.Errorf("an entry level with the database was reported as a violation: %+v", v)
	}
}

func TestAnEntryAheadOfTheDatabaseIsNotAViolation(t *testing.T) {
	t.Parallel()

	// This happens legitimately: the verifier read the cache after a write landed and the database
	// before it. Reporting it would make the verifier's own read ordering look like a cache bug.
	obs := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 90, FillVersion: 100,
		BehindSince: at(0), Now: at(30),
	}
	if v, is := sextant.Classify(obs, bound()); is {
		t.Errorf("an entry ahead of the database was reported as a violation: %+v", v)
	}
}

func TestAnInFlightRaceIsNotAViolation(t *testing.T) {
	t.Parallel()

	// The case that decides whether this is usable. The entry IS behind, but only for a moment —
	// an invalidation is in flight and the model never promised otherwise. Counting this would make
	// every healthy system report violations continuously.
	p := bound()
	obs := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 200, FillVersion: 100,
		BehindSince: at(0),
		Now:         at(0).Add(p.Duration() - time.Millisecond),
	}
	if v, is := sextant.Classify(obs, p); is {
		t.Errorf("an entry behind for less than the propagation bound was reported as a violation: %+v", v)
	}
}

func TestStaleBeyondThePropagationBoundIsAViolation(t *testing.T) {
	t.Parallel()

	// Past P, "in flight" stops being an explanation. Something was dropped, reordered or lost.
	p := bound()
	obs := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 200, FillVersion: 100,
		BehindSince: at(0),
		Now:         at(0).Add(p.Duration() + time.Millisecond),
	}
	v, is := sextant.Classify(obs, p)
	if !is {
		t.Fatal("an entry behind for longer than the propagation bound was not reported as a violation")
	}
	if v.Key != "entities:1" || v.DBVersion != 200 || v.FillVersion != 100 {
		t.Errorf("the violation does not carry what is needed to investigate it: %+v", v)
	}
	if v.Behind <= 0 {
		t.Error("the violation reports no duration; how long it was wrong is the first thing anyone asks")
	}
}

func TestExactlyAtTheBoundIsNotYetAViolation(t *testing.T) {
	t.Parallel()

	// The boundary is inclusive of the promise: the model says propagation completes WITHIN P, so
	// an entry at exactly P has not yet broken it. Choosing the other side would report a violation
	// for a system that met its stated bound precisely.
	p := bound()
	obs := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 200, FillVersion: 100,
		BehindSince: at(0), Now: at(0).Add(p.Duration()),
	}
	if _, is := sextant.Classify(obs, p); is {
		t.Error("an entry behind for exactly the propagation bound was reported as a violation")
	}
}

func TestAViolationIsAttributedToTheRightLevels(t *testing.T) {
	t.Parallel()

	// A single stale entry violates some levels and not others (§7). Blending them into one number
	// would hide exactly the trade the levels exist to expose — which is the whole reason the SLO
	// is per level.
	p := bound()
	obs := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 200, FillVersion: 100,
		BehindSince: at(0), Now: at(0).Add(p.Duration() + time.Second),
		Watermark: 150, WatermarkKnown: true,
	}
	v, is := sextant.Classify(obs, p)
	if !is {
		t.Fatal("precondition: expected a violation")
	}

	// STRONG never reads the cache, so a stale entry cannot violate it. A STRONG read returning a
	// stale value would be a DATABASE bug, and attributing it here would point every investigation
	// at the wrong component.
	if v.Violates(consistency.Strong) {
		t.Error("a stale cache entry was attributed to STRONG, which never reads the cache")
	}

	// SESSION is violated because an entry at fv=100 was served to a session holding a watermark of
	// 150 — the watermark check was bypassed or wrong.
	if !v.Violates(consistency.Session) {
		t.Error("an entry below the session watermark was not attributed to SESSION")
	}
}

func TestAStaleEntryAboveTheWatermarkDoesNotViolateSession(t *testing.T) {
	t.Parallel()

	// The entry is behind the database but still at or above what this session has observed. SESSION
	// promises read-own-writes and monotonic reads, not read-others-writes, so this is exactly the
	// staleness the level is documented to allow. Counting it would make SESSION's number describe a
	// promise it never made.
	p := bound()
	obs := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 200, FillVersion: 150,
		BehindSince: at(0), Now: at(0).Add(p.Duration() + time.Second),
		Watermark: 150, WatermarkKnown: true,
	}
	v, is := sextant.Classify(obs, p)
	if !is {
		t.Fatal("precondition: expected a violation of the weaker levels")
	}
	if v.Violates(consistency.Session) {
		t.Error("an entry at the session watermark was attributed to SESSION")
	}
	// EVENTUAL is still violated: the entry never converged within the bound.
	if !v.Violates(consistency.Eventual) {
		t.Error("an entry that never converged was not attributed to EVENTUAL")
	}
}

func TestWithNoWatermarkSessionIsNotAccused(t *testing.T) {
	t.Parallel()

	// Sampling observes keys without any session context. Assuming a watermark we do not have would
	// invent SESSION violations out of missing information — and SESSION is the default level, so
	// that number is the one people will react to hardest.
	p := bound()
	obs := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 200, FillVersion: 100,
		BehindSince: at(0), Now: at(0).Add(p.Duration() + time.Second),
		WatermarkKnown: false,
	}
	v, is := sextant.Classify(obs, p)
	if !is {
		t.Fatal("precondition: expected a violation")
	}
	if v.Violates(consistency.Session) {
		t.Error("SESSION was accused with no watermark observed; the verifier invented information")
	}
}

func TestBoundedIsViolatedOnlyPastItsOwnWindow(t *testing.T) {
	t.Parallel()

	p := bound()
	base := sextant.Observation{
		Key: "entities:1", Shard: "shard0",
		DBVersion: 200, FillVersion: 100,
		BehindSince: at(0),
	}

	// Inside t + max_clock_skew, a BOUNDED(t) read is being served exactly what it asked for.
	within := base
	within.Now = at(0).Add(p.Duration() + time.Second)
	within.BoundedWindow = time.Hour
	v, is := sextant.Classify(within, p)
	if !is {
		t.Fatal("precondition: expected a violation of the weaker levels")
	}
	if v.Violates(consistency.Bounded) {
		t.Error("BOUNDED was accused inside its own window")
	}

	// Past it, the promise is broken.
	past := base
	past.Now = at(0).Add(p.Duration() + time.Second)
	past.BoundedWindow = time.Millisecond
	v, is = sextant.Classify(past, p)
	if !is {
		t.Fatal("precondition: expected a violation")
	}
	if !v.Violates(consistency.Bounded) {
		t.Error("BOUNDED was not accused past its own window")
	}
}

func TestAViolationExplainsItself(t *testing.T) {
	t.Parallel()

	p := bound()
	obs := sextant.Observation{
		Key: "entities:42", Shard: "shard1",
		DBVersion: 200, FillVersion: 100,
		BehindSince: at(0), Now: at(0).Add(p.Duration() + time.Second),
	}
	v, is := sextant.Classify(obs, p)
	if !is {
		t.Fatal("precondition: expected a violation")
	}

	// The difference between a monitor and a verifier, rendered. Someone woken at 3am must be able
	// to read this line and know which key, which shard, how far behind, and for how long.
	got := v.String()
	for _, want := range []string{"entities:42", "shard1", "100", "200"} {
		if !contains(got, want) {
			t.Errorf("Violation.String() = %q, missing %q", got, want)
		}
	}
}
