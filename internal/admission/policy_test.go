package admission_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/admission"
)

// Oscillation is the NAMED risk for this milestone, and the reason hysteresis and dwell exist.
//
// A key sitting near the threshold with a single number would flip on every sample: admitted,
// evicted, admitted, evicted. Each flip is a wasted fill and a wasted invalidation, so a policy that
// oscillates is strictly worse than caching everything — it pays all the costs of caching and
// collects none of the benefit. These tests hold the two mitigations to account.

func policy() admission.Policy {
	// Two thresholds, not one. A key must reach 20:1 to be admitted but only falls out below 10:1,
	// so the band between them is where a borderline key sits still instead of flipping.
	return admission.NewPolicy(admission.PolicyOptions{
		AdmitRatio:   20,
		EvictRatio:   10,
		MinSamples:   20,
		MinDwell:     30 * time.Second,
		DefaultAdmit: true,
	})
}

func TestAKeyWithNoEvidenceTakesTheDefault(t *testing.T) {
	t.Parallel()

	// A cold key has no ratio yet. Refusing to cache it would mean nothing is ever cached, because
	// a key cannot build a read history without being read — and it cannot be read from a cache it
	// was never admitted to.
	p := policy()
	d := p.Decide(admission.State{Key: "entities:1", Reads: 0, Writes: 0, Now: base()})

	if !d.Admit {
		t.Error("a key with no observations was refused; nothing would ever be cached")
	}
	if d.Reason == "" {
		t.Error("the decision carries no reason; `cachetctl admission explain` would have nothing to say")
	}
}

func TestTooFewSamplesTakesTheDefault(t *testing.T) {
	t.Parallel()

	// Three reads and one write is a 3:1 ratio and means nothing. Acting on it would evict a key on
	// the strength of four observations.
	p := policy()
	d := p.Decide(admission.State{Key: "entities:1", Reads: 3, Writes: 1, Now: base()})

	if !d.Admit {
		t.Errorf("a key with 4 observations was evicted on that evidence: %s", d.Reason)
	}
}

func TestAReadHeavyKeyIsAdmitted(t *testing.T) {
	t.Parallel()

	p := policy()
	d := p.Decide(admission.State{Key: "entities:1", Reads: 1000, Writes: 10, Now: base()})

	if !d.Admit {
		t.Errorf("a 100:1 key was not admitted: %s", d.Reason)
	}
}

func TestAWriteChurningKeyIsEvicted(t *testing.T) {
	t.Parallel()

	// The case the whole milestone exists for: a write-churning key inside an otherwise read-heavy
	// table. Every write pays invalidation, every read misses, and a human picking tables would
	// never find it.
	p := policy()
	d := p.Decide(admission.State{
		Key: "entities:1", Reads: 100, Writes: 100, Now: base(),
		Admitted: true, Since: base().Add(-time.Hour),
	})

	if d.Admit {
		t.Error("a 1:1 key stayed admitted; it pays invalidation on every write and misses on every read")
	}
	if d.Reason == "" {
		t.Error("the eviction carries no reason")
	}
}

func TestTheHysteresisBandHoldsAKeyStill(t *testing.T) {
	t.Parallel()

	// The central anti-oscillation property. A key at 15:1 is below the 20:1 admit threshold and
	// above the 10:1 evict threshold. Whichever state it is in, it stays there.
	p := policy()
	now := base()

	admitted := p.Decide(admission.State{
		Key: "entities:1", Reads: 150, Writes: 10, Now: now,
		Admitted: true, Since: now.Add(-time.Hour),
	})
	if !admitted.Admit {
		t.Errorf("an admitted key at 15:1 was evicted despite being above the evict threshold: %s", admitted.Reason)
	}

	notAdmitted := p.Decide(admission.State{
		Key: "entities:1", Reads: 150, Writes: 10, Now: now,
		Admitted: false, Since: now.Add(-time.Hour),
	})
	if notAdmitted.Admit {
		t.Errorf("an unadmitted key at 15:1 was admitted despite being below the admit threshold: %s",
			notAdmitted.Reason)
	}
}

func TestMinimumDwellPreventsRapidFlipping(t *testing.T) {
	t.Parallel()

	// The second mitigation, and the one that covers a key whose ratio swings clean through the
	// band. Even a decisive change may not take effect until the current state has been held long
	// enough, so the worst achievable flip rate is bounded by configuration rather than by traffic.
	p := policy()
	now := base()

	d := p.Decide(admission.State{
		Key: "entities:1", Reads: 10, Writes: 100, Now: now,
		Admitted: true, Since: now.Add(-time.Second), // admitted one second ago
	})
	if !d.Admit {
		t.Errorf("a key admitted one second ago was evicted inside the 30s dwell: %s", d.Reason)
	}

	// Past the dwell, the same evidence takes effect.
	later := p.Decide(admission.State{
		Key: "entities:1", Reads: 10, Writes: 100, Now: now,
		Admitted: true, Since: now.Add(-time.Minute),
	})
	if later.Admit {
		t.Errorf("a decisively write-heavy key stayed admitted past its dwell: %s", later.Reason)
	}
}

func TestASustainedWriteHeavyKeyStaysEvicted(t *testing.T) {
	t.Parallel()

	// The milestone's stated exit criterion: evicted, and STAYS evicted. A policy that evicted and
	// then readmitted on the next sample would be the oscillation this is built to avoid, wearing
	// the appearance of working.
	p := policy()
	now := base()
	state := admission.State{Key: "entities:1", Reads: 50, Writes: 100, Now: now, Admitted: true, Since: now.Add(-time.Hour)}

	for i := 0; i < 20; i++ {
		d := p.Decide(state)
		if d.Admit {
			t.Fatalf("round %d readmitted a sustained 1:2 key: %s", i, d.Reason)
		}
		// Carry the decision forward, as the real caller does.
		if state.Admitted != d.Admit {
			state.Since = now
		}
		state.Admitted = d.Admit
		now = now.Add(time.Minute)
		state.Now = now
	}
}

func TestADecisionExplainsItself(t *testing.T) {
	t.Parallel()

	// `cachetctl admission explain` is in the delivery model for a reason: a cache that silently
	// declines to cache a key is indistinguishable from a cache that is broken, and the first thing
	// anyone asks is "why is this key not cached?".
	p := policy()
	d := p.Decide(admission.State{
		Key: "entities:1", Reads: 100, Writes: 100, Now: base(),
		Admitted: true, Since: base().Add(-time.Hour),
	})

	if d.Ratio < 0.9 || d.Ratio > 1.1 {
		t.Errorf("Ratio = %v for 100 reads and 100 writes, want about 1", d.Ratio)
	}
	if !strings.Contains(d.Reason, "10") {
		t.Errorf("Reason = %q, and does not mention the threshold it failed", d.Reason)
	}
}

func TestZeroWritesIsNotDivisionByZero(t *testing.T) {
	t.Parallel()

	// A read-only key is the single most cacheable thing there is, and it is also the one that
	// divides by zero.
	p := policy()
	d := p.Decide(admission.State{Key: "entities:1", Reads: 500, Writes: 0, Now: base()})

	if !d.Admit {
		t.Errorf("a key with no writes at all was not admitted: %s", d.Reason)
	}
}

func TestAnInvertedConfigurationIsRejected(t *testing.T) {
	t.Parallel()

	// An evict threshold above the admit threshold inverts the band: a key would be admitted and
	// immediately eligible for eviction, which is a configuration that guarantees the oscillation
	// the band exists to prevent.
	if _, err := admission.NewPolicyChecked(admission.PolicyOptions{
		AdmitRatio: 10, EvictRatio: 20, MinSamples: 20, MinDwell: time.Second,
	}); err == nil {
		t.Error("a policy with evict_ratio above admit_ratio was accepted")
	}
}
