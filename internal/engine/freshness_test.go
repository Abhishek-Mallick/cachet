package engine_test

import (
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// now is a fixed instant so the freshness rules are tested rather than the clock.
var now = time.UnixMilli(1_700_000_000_000)

func versionAt(t time.Time) storage.Version {
	return storage.NewVersion(t.UnixMilli(), 0)
}

func TestStrongNeverAcceptsACachedEntry(t *testing.T) {
	t.Parallel()

	// STRONG bypasses the cache entirely. If it ever accepted an entry, the one level that is
	// supposed to be un-cacheable would silently become cacheable.
	ok := engine.AcceptEntry(engine.Freshness{
		Requirement: consistency.Requirement{Level: consistency.Strong},
		FillVersion: versionAt(now),
		Now:         now,
	})
	if ok {
		t.Error("AcceptEntry accepted an entry at STRONG")
	}
}

func TestEventualAcceptsAnyEntry(t *testing.T) {
	t.Parallel()

	// EVENTUAL is the control group: it declines read-own-writes so the cost of the other levels is
	// visible in the benchmark table.
	ok := engine.AcceptEntry(engine.Freshness{
		Requirement:    consistency.Requirement{Level: consistency.Eventual},
		FillVersion:    versionAt(now.Add(-24 * time.Hour)),
		Watermark:      uint64(versionAt(now)),
		WatermarkKnown: true,
		Now:            now,
	})
	if !ok {
		t.Error("EVENTUAL rejected an entry; it should accept anything not tombstoned")
	}
}

func TestSessionRejectsAnEntryOlderThanTheWatermark(t *testing.T) {
	t.Parallel()

	// This is read-own-writes. The session wrote at T, so an entry filled from a database state
	// before T cannot be served to it.
	ok := engine.AcceptEntry(engine.Freshness{
		Requirement:    consistency.Requirement{Level: consistency.Session},
		FillVersion:    versionAt(now.Add(-time.Second)),
		Watermark:      uint64(versionAt(now)),
		WatermarkKnown: true,
		Now:            now,
	})
	if ok {
		t.Error("SESSION served an entry filled before the session's own write")
	}
}

func TestSessionAcceptsAnEntryAtOrAfterTheWatermark(t *testing.T) {
	t.Parallel()

	for _, delta := range []time.Duration{0, time.Millisecond, time.Hour} {
		ok := engine.AcceptEntry(engine.Freshness{
			Requirement:    consistency.Requirement{Level: consistency.Session},
			FillVersion:    versionAt(now.Add(delta)),
			Watermark:      uint64(versionAt(now)),
			WatermarkKnown: true,
			Now:            now.Add(delta),
		})
		if !ok {
			t.Errorf("SESSION rejected an entry filled %s after the watermark", delta)
		}
	}
}

func TestSessionWithNoWatermarkAcceptsAnyEntry(t *testing.T) {
	t.Parallel()

	// A session that has written nothing to this shard has no writes to read, so there is nothing
	// for the watermark to protect. Treating an absent watermark as "reject everything" would
	// collapse the hit rate for every read-only client.
	ok := engine.AcceptEntry(engine.Freshness{
		Requirement: consistency.Requirement{Level: consistency.Session},
		FillVersion: versionAt(now.Add(-time.Hour)),
		Now:         now,
	})
	if !ok {
		t.Error("SESSION rejected an entry although the session holds no watermark for the shard")
	}
}

func TestBoundedRejectsAnEntryOlderThanTheWindow(t *testing.T) {
	t.Parallel()

	ok := engine.AcceptEntry(engine.Freshness{
		Requirement:  consistency.Requirement{Level: consistency.Bounded, StalenessBound: 5 * time.Second},
		FillVersion:  versionAt(now.Add(-10 * time.Second)),
		Now:          now,
		MaxClockSkew: 250 * time.Millisecond,
	})
	if ok {
		t.Error("BOUNDED(5s) served an entry filled 10s ago")
	}
}

func TestBoundedAcceptsAnEntryInsideTheWindow(t *testing.T) {
	t.Parallel()

	ok := engine.AcceptEntry(engine.Freshness{
		Requirement:  consistency.Requirement{Level: consistency.Bounded, StalenessBound: 5 * time.Second},
		FillVersion:  versionAt(now.Add(-1 * time.Second)),
		Now:          now,
		MaxClockSkew: 250 * time.Millisecond,
	})
	if !ok {
		t.Error("BOUNDED(5s) rejected an entry filled 1s ago")
	}
}

func TestBoundedWindowIsShortenedByClockSkew(t *testing.T) {
	t.Parallel()

	// The engine is conservative about its own clock: an under-estimate of an entry's age must
	// never widen the window that was promised (CONSISTENCY.md §3.3). An entry 4.9s old is inside a
	// 5s bound on a perfect clock, and outside it once 250ms of possible skew is accounted for.
	f := engine.Freshness{
		Requirement:  consistency.Requirement{Level: consistency.Bounded, StalenessBound: 5 * time.Second},
		FillVersion:  versionAt(now.Add(-4900 * time.Millisecond)),
		Now:          now,
		MaxClockSkew: 250 * time.Millisecond,
	}
	if engine.AcceptEntry(f) {
		t.Error("BOUNDED accepted an entry that only fits inside the window if the clocks agree exactly")
	}

	f.MaxClockSkew = 0
	if !engine.AcceptEntry(f) {
		t.Error("with no skew allowance the same entry should fit inside the window")
	}
}

func TestBoundedAlsoEnforcesTheSessionWatermark(t *testing.T) {
	t.Parallel()

	// BOUNDED is SESSION plus a freshness floor; it is never weaker than SESSION for your own
	// writes. A fresh entry that predates your own write must still be rejected.
	ok := engine.AcceptEntry(engine.Freshness{
		Requirement:    consistency.Requirement{Level: consistency.Bounded, StalenessBound: time.Hour},
		FillVersion:    versionAt(now.Add(-time.Second)),
		Watermark:      uint64(versionAt(now)),
		WatermarkKnown: true,
		Now:            now,
	})
	if ok {
		t.Error("BOUNDED served an entry filled before the session's own write")
	}
}
