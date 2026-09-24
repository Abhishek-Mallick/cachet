package sextant

import (
	"math"
	"sync"
	"time"

	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// SLO accumulates observations into a measured consistency figure per level.
//
// Per level and never blended. A single number would average a level that bypasses the cache
// entirely with one promising only eventual convergence; the result would describe no promise
// Cachet actually makes, while hiding the exact trade the levels exist to expose.
//
// The window rolls, because an SLO that never forgets is one nobody can bring back to green — so
// after the first bad day nobody tries.
type SLO struct {
	window time.Duration

	mu     sync.Mutex
	levels map[consistency.Level]*samples
}

type samples struct {
	// observations are sorted by time, because they arrive that way and expiry only ever trims the
	// front.
	observations []sample
}

type sample struct {
	at        time.Time
	violation bool
}

// Report is a measured consistency figure for one level.
type Report struct {
	Level        consistency.Level
	Observations int
	Violations   int

	// Consistency is the fraction of observations that were not violations.
	Consistency float64

	// Nines is -log10(1 - Consistency), the form this gets discussed in. Computed here rather than
	// left to each consumer, so two dashboards cannot disagree about the same window.
	Nines float64

	// Known distinguishes "measured 100%" from "measured nothing".
	//
	// This is the field that keeps the dashboard honest at startup. Zero observations is not perfect
	// consistency, it is no evidence — and a verifier that reports a perfect score before it has
	// looked at anything is the most misleading thing this component could do.
	Known bool
}

// NewSLO builds an SLO over a rolling window.
func NewSLO(window time.Duration) *SLO {
	if window <= 0 {
		window = time.Minute
	}
	return &SLO{window: window, levels: make(map[consistency.Level]*samples)}
}

// Observe records one checked read at a level.
//
// STRONG is ignored: it does not read the cache, so a cache violation cannot be attributed to it.
// Silently dropping it rather than trusting the caller means a mistake elsewhere cannot corrupt the
// one number a reader trusts most.
func (s *SLO) Observe(level consistency.Level, at time.Time, violation bool) {
	if level == consistency.Strong {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	set, ok := s.levels[level]
	if !ok {
		set = &samples{}
		s.levels[level] = set
	}
	set.observations = append(set.observations, sample{at: at, violation: violation})
}

// Report returns the figure for a level as of now.
func (s *SLO) Report(level consistency.Level, now time.Time) Report {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := Report{Level: level}
	set, ok := s.levels[level]
	if !ok {
		return r
	}

	cutoff := now.Add(-s.window)
	keep := 0
	for _, obs := range set.observations {
		if obs.at.After(cutoff) {
			set.observations[keep] = obs
			keep++
		}
	}
	set.observations = set.observations[:keep]

	for _, obs := range set.observations {
		r.Observations++
		if obs.violation {
			r.Violations++
		}
	}
	if r.Observations == 0 {
		return r
	}

	r.Known = true
	r.Consistency = float64(r.Observations-r.Violations) / float64(r.Observations)

	// A clean window is infinite nines mathematically, which renders as "+Inf" and makes a panel
	// look broken at exactly the moment everything is fine.
	//
	// Report what the sample size actually supports instead, via the rule of three: observing zero
	// failures in n trials puts the 95% upper bound on the failure rate at 3/n, so the evidence
	// supports log10(n/3) nines and no more. Reporting log10(n) — the naive choice — would claim a
	// third of an order of magnitude more reliability than was measured, every time the window is
	// clean. On a product whose entire pitch is that its numbers can be checked, the optimistic
	// version is the one that loses an argument.
	switch {
	case r.Violations > 0:
		r.Nines = -math.Log10(1 - r.Consistency)
	case r.Observations > 3:
		r.Nines = math.Log10(float64(r.Observations) / 3)
	default:
		// Three or fewer clean observations support no claim at all.
		r.Nines = 0
	}
	return r
}

// Levels returns the levels that have been observed, so a caller can export every one it has
// evidence for without hardcoding the list.
func (s *SLO) Levels() []consistency.Level {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]consistency.Level, 0, len(s.levels))
	for l := range s.levels {
		out = append(out, l)
	}
	return out
}
