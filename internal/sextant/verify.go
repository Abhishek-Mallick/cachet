package sextant

import (
	"fmt"
	"time"

	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// PropagationBound is how long an entry may legitimately lag the database.
//
// It is the line between a benign in-flight race and a real violation, and getting it wrong in
// either direction destroys the product. Too tight and every healthy system reports violations
// continuously, the number becomes noise, and nobody opens the dashboard again. Too loose and real
// staleness reads as healthy — which is worse than shipping no verifier at all, because it arrives
// with a reassuring green number attached.
//
// It is SUMMED FROM CONFIGURATION rather than tuned by hand (CONSISTENCY.md §7), so the threshold is
// derived from what the engine was actually promising. A hand-tuned constant would drift away from
// the guarantee the moment anyone changed a setting, and nothing would report the drift.
type PropagationBound struct {
	writePathBudget time.Duration
	cdcLagBound     time.Duration
	maxClockSkew    time.Duration
}

// NewPropagationBound builds the bound from the three guarantee settings that determine it.
func NewPropagationBound(writePathBudget, cdcLagBound, maxClockSkew time.Duration) PropagationBound {
	return PropagationBound{
		writePathBudget: writePathBudget,
		cdcLagBound:     cdcLagBound,
		maxClockSkew:    maxClockSkew,
	}
}

// Duration is the total bound.
func (p PropagationBound) Duration() time.Duration {
	return p.writePathBudget + p.cdcLagBound + p.maxClockSkew
}

// MaxClockSkew is the skew allowance, which BOUNDED(t) accounting needs separately.
func (p PropagationBound) MaxClockSkew() time.Duration { return p.maxClockSkew }

// Observation is one sampled comparison of a cache entry against the database.
type Observation struct {
	Key   string
	Shard string

	// DBVersion is the row's version in the database; FillVersion is the database state the cached
	// entry was filled from. Comparing FILL version rather than row version is what makes this
	// answer "how stale is the snapshot behind this entry", which is the question the guarantee is
	// written in terms of (CONSISTENCY.md §1).
	DBVersion   uint64
	FillVersion uint64

	// BehindSince is when the entry was first seen behind the database, and Now is the moment of
	// this observation. The pair is what makes the propagation bound a DURATION rather than a
	// guess — a single sample cannot tell a momentary race from a permanent one.
	BehindSince time.Time
	Now         time.Time

	// Watermark is the session watermark observed for this shard, when one was. Sampling usually
	// has none, and assuming one would invent SESSION violations out of missing information.
	Watermark      uint64
	WatermarkKnown bool

	// BoundedWindow is the t of a BOUNDED(t) read being accounted for, when one is.
	BoundedWindow time.Duration

	// Trace is the mutation history, attached so the violation explains itself.
	Trace []Event
}

// Violation is an entry that stayed behind the database for longer than the propagation bound.
type Violation struct {
	Key   string
	Shard string

	DBVersion   uint64
	FillVersion uint64

	// Behind is how long the entry had been stale when it was observed.
	Behind time.Duration

	// Levels are the consistency levels this violates. A single stale entry violates some and not
	// others, and blending them into one number would hide exactly the trade the levels exist to
	// expose (CONSISTENCY.md §7).
	Levels []consistency.Level

	// Trace explains it. This field is the difference between a monitor and a verifier.
	Trace []Event
}

// Violates reports whether this violation counts against a level.
func (v Violation) Violates(l consistency.Level) bool {
	for _, got := range v.Levels {
		if got == l {
			return true
		}
	}
	return false
}

// String renders the violation for someone reading it during an incident.
func (v Violation) String() string {
	return fmt.Sprintf("%s on %s: cache fv=%d, db=%d, behind %s, violates %v",
		v.Key, v.Shard, v.FillVersion, v.DBVersion, v.Behind.Round(time.Millisecond), v.Levels)
}

// Classify decides whether an observation is a violation, and of which levels.
//
// This is CONSISTENCY.md §7's definition and nothing more: the rule lives in one function so that
// the number on the dashboard and the sentence in the document cannot drift apart.
func Classify(obs Observation, p PropagationBound) (Violation, bool) {
	// Not behind at all — including the case where the entry is AHEAD, which happens legitimately
	// when the verifier read the cache after a write landed and the database before it. Reporting
	// that would make the verifier's own read ordering look like a cache bug.
	if obs.FillVersion >= obs.DBVersion {
		return Violation{}, false
	}

	behind := obs.Now.Sub(obs.BehindSince)

	// Inclusive of the promise: the model says propagation completes WITHIN P, so an entry at
	// exactly P has not broken it. Choosing the other side would report a violation against a system
	// that met its stated bound precisely.
	if behind <= p.Duration() {
		return Violation{}, false
	}

	v := Violation{
		Key:         obs.Key,
		Shard:       obs.Shard,
		DBVersion:   obs.DBVersion,
		FillVersion: obs.FillVersion,
		Behind:      behind,
		Trace:       obs.Trace,
	}

	// EVENTUAL: the entry never converged within the bound. Every violation counts here, because
	// convergence is the only thing EVENTUAL promises.
	v.Levels = append(v.Levels, consistency.Eventual)

	// SESSION: violated only when an entry BELOW the session's watermark was servable. SESSION
	// promises read-own-writes and monotonic reads, never read-others-writes — an entry that is
	// stale relative to the database but at or above what this session has observed is exactly the
	// staleness the level documents as permitted.
	if obs.WatermarkKnown && obs.FillVersion < obs.Watermark {
		v.Levels = append(v.Levels, consistency.Session)
	}

	// BOUNDED(t): violated only past its own window, widened by the clock-skew allowance so the
	// engine stays conservative about its own clock.
	if obs.BoundedWindow > 0 && behind > obs.BoundedWindow+p.MaxClockSkew() {
		v.Levels = append(v.Levels, consistency.Bounded)
	}

	// STRONG is never added. It does not read the cache, so a stale entry cannot violate it — a
	// STRONG read returning a stale value is a DATABASE bug, and attributing it here would point
	// every investigation at the wrong component.

	return v, true
}
