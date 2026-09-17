package admission

import (
	"fmt"
	"math"
	"time"
)

// PolicyOptions configures the admission decision.
type PolicyOptions struct {
	// AdmitRatio and EvictRatio are two thresholds, not one, and the gap between them is the
	// hysteresis band.
	//
	// Oscillation is the named risk for this whole mechanism. With a single threshold, a key sitting
	// near it flips on every sample — admitted, evicted, admitted — and every flip is a wasted fill
	// plus a wasted invalidation. A policy that oscillates is strictly WORSE than caching
	// everything: it pays all the costs and collects none of the benefit. The band is what lets a
	// borderline key sit still.
	AdmitRatio float64
	EvictRatio float64

	// MinSamples is the evidence floor. Three reads and one write is a 3:1 ratio and means nothing;
	// acting on it would evict a key on the strength of four observations.
	MinSamples uint32

	// MinDwell is how long a key holds its current state before a change can take effect.
	//
	// The second anti-oscillation mitigation, and the one that covers a key whose ratio swings clean
	// through the band rather than sitting inside it. With it, the worst achievable flip rate is
	// bounded by configuration rather than by the workload.
	MinDwell time.Duration

	// DefaultAdmit is what happens to a key with too little evidence.
	//
	// True, and it has to be: a key cannot build a read history without being read, and it cannot be
	// read from a cache it was never admitted to. Defaulting to "no" would mean nothing is ever
	// cached — the mechanism would starve itself.
	DefaultAdmit bool
}

// State is what is known about a key at decision time.
type State struct {
	Key           string
	Reads, Writes uint32

	// Admitted is the key's current state, and Since is when it entered it. Both are needed: the
	// decision depends on where the key IS, not only on its ratio, which is what hysteresis means.
	Admitted bool
	Since    time.Time

	Now time.Time
}

// Decision is an admission outcome, with its reasoning attached.
type Decision struct {
	Key   string
	Admit bool

	// Ratio is reads per write over the window.
	Ratio float64

	// Reason is why, in a form a human can read.
	//
	// `cachetctl admission explain` exists because a cache that silently declines to cache a key is
	// indistinguishable from a cache that is broken, and "why is this key not cached?" is the first
	// question anyone asks. A decision that cannot answer it is a decision nobody will trust.
	Reason string
}

// Policy decides whether a key is worth caching.
type Policy struct{ opts PolicyOptions }

// NewPolicy builds a Policy, normalising anything unusable.
func NewPolicy(opts PolicyOptions) Policy {
	if opts.AdmitRatio <= 0 {
		opts.AdmitRatio = 20
	}
	if opts.EvictRatio <= 0 || opts.EvictRatio > opts.AdmitRatio {
		opts.EvictRatio = opts.AdmitRatio / 2
	}
	if opts.MinSamples == 0 {
		opts.MinSamples = 20
	}
	if opts.MinDwell < 0 {
		opts.MinDwell = 0
	}
	return Policy{opts: opts}
}

// NewPolicyChecked builds a Policy, rejecting a configuration that cannot work.
//
// An evict threshold above the admit threshold inverts the band: a key would be admitted and be
// immediately eligible for eviction, guaranteeing the oscillation the band exists to prevent. That
// has to be a boot error, not a silently corrected value, because an operator who wrote it believes
// something about their system that is not true.
func NewPolicyChecked(opts PolicyOptions) (Policy, error) {
	if opts.AdmitRatio <= 0 {
		return Policy{}, fmt.Errorf("admission: admit_ratio must be positive, got %v", opts.AdmitRatio)
	}
	if opts.EvictRatio <= 0 {
		return Policy{}, fmt.Errorf("admission: evict_ratio must be positive, got %v", opts.EvictRatio)
	}
	if opts.EvictRatio > opts.AdmitRatio {
		return Policy{}, fmt.Errorf(
			"admission: evict_ratio (%v) is above admit_ratio (%v), which inverts the hysteresis band "+
				"and guarantees oscillation", opts.EvictRatio, opts.AdmitRatio)
	}
	if opts.MinDwell < 0 {
		return Policy{}, fmt.Errorf("admission: min_dwell must not be negative, got %s", opts.MinDwell)
	}
	return NewPolicy(opts), nil
}

// Decide returns whether a key should be cached.
func (p Policy) Decide(s State) Decision {
	ratio := Ratio(s.Reads, s.Writes)
	d := Decision{Key: s.Key, Ratio: ratio, Admit: s.Admitted}

	total := s.Reads + s.Writes
	if total < p.opts.MinSamples {
		d.Admit = p.opts.DefaultAdmit
		d.Reason = fmt.Sprintf("%d observations is below the %d needed to decide; taking the default",
			total, p.opts.MinSamples)
		return d
	}

	// Dwell first: a key that changed state recently keeps it regardless of the evidence, which is
	// what bounds the flip rate when a ratio swings clean through the band.
	if p.opts.MinDwell > 0 && !s.Since.IsZero() {
		if held := s.Now.Sub(s.Since); held < p.opts.MinDwell {
			d.Reason = fmt.Sprintf("held for %s, under the %s minimum dwell; ratio %.1f:1 not yet acted on",
				held.Round(time.Second), p.opts.MinDwell, ratio)
			return d
		}
	}

	switch {
	case !s.Admitted && ratio >= p.opts.AdmitRatio:
		d.Admit = true
		d.Reason = fmt.Sprintf("%.1f:1 read:write is at or above the %.0f:1 admit threshold", ratio, p.opts.AdmitRatio)
	case s.Admitted && ratio < p.opts.EvictRatio:
		d.Admit = false
		d.Reason = fmt.Sprintf("%.1f:1 read:write is below the %.0f:1 evict threshold; every write pays "+
			"invalidation and every read misses", ratio, p.opts.EvictRatio)
	default:
		// Inside the band. This is the case hysteresis exists for, and doing nothing is the whole
		// point of it.
		d.Reason = fmt.Sprintf("%.1f:1 read:write is inside the %.0f–%.0f hysteresis band; unchanged",
			ratio, p.opts.EvictRatio, p.opts.AdmitRatio)
	}
	return d
}

// Ratio is reads per write, with no writes treated as the most cacheable case there is rather than
// as a division by zero.
func Ratio(reads, writes uint32) float64 {
	if writes == 0 {
		if reads == 0 {
			return 0
		}
		return math.Inf(1)
	}
	return float64(reads) / float64(writes)
}
