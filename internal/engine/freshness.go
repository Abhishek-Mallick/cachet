package engine

import (
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// Freshness is everything needed to decide whether a cached entry may be served.
//
// It is a plain struct evaluated by a pure function on purpose. This decision IS the consistency
// model at request time, so it has to be testable without a database, a cache, or a clock — and
// every rule in CONSISTENCY.md §3 has a named test against it.
type Freshness struct {
	Requirement consistency.Requirement

	// FillVersion is the shard-state version the entry reflects — not the row's own version. The
	// distinction is what keeps SESSION from collapsing the hit rate on any shard taking writes
	// (CONSISTENCY.md §1).
	FillVersion storage.Version

	// Watermark is the session's position on this entry's shard. WatermarkKnown separates "this
	// session never wrote here" from "it did and we know where".
	Watermark      uint64
	WatermarkKnown bool

	Now          time.Time
	MaxClockSkew time.Duration
}

// AcceptEntry reports whether a cached entry satisfies the request's consistency requirement.
//
// A false result is not an error: it means the entry is treated as a miss and the read falls
// through to the database. That is how every level above EVENTUAL stays correct while still using
// the cache for everything it legitimately can.
func AcceptEntry(f Freshness) bool {
	// STRONG never consults the cache. Reaching here at all would be a bug in the caller, so the
	// check is repeated rather than assumed.
	if f.Requirement.Level.BypassesCache() {
		return false
	}

	if f.Requirement.Level.RequiresWatermark() && f.WatermarkKnown {
		// Read-own-writes: an entry filled from a database state older than something this session
		// has already written or observed cannot be served to it.
		if uint64(f.FillVersion) < f.Watermark {
			return false
		}
	}

	if f.Requirement.Level == consistency.Bounded {
		// The skew allowance is SUBTRACTED from the window rather than added to it. The engine is
		// conservative about its own clock, so an under-estimate of the entry's age can never widen
		// the guarantee that was promised (CONSISTENCY.md §3.3).
		oldest := f.Now.Add(-f.Requirement.StalenessBound).Add(f.MaxClockSkew)
		if f.FillVersion.Time().Before(oldest) {
			return false
		}
	}

	return true
}
