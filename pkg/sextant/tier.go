package sextant

import "fmt"

// How much a verifier can actually know about the cache it is checking.
//
// Cachet's own entries carry a hybrid logical clock fill version, so a comparison against the
// database is exact. A foreign Redis holding somebody's JSON has no such thing, and the honest
// answer is that some guarantees cannot be checked there at all.
//
// The tier is therefore not a footnote. It is a label on every metric this package exports, so a
// run that could only compare values cannot be read as a BOUNDED claim — which is the failure that
// would make the whole tool worse than useless, because it would arrive with a reassuring number
// attached.

// Tier is how a verifier compares a cache entry against the origin.
type Tier string

const (
	// TierValue compares a cached value against a projection of the database row.
	//
	// It answers "is this entry stale, and for how long" and nothing else. Without a version there
	// is no way to say which database state an entry reflects, so SESSION and BOUNDED cannot be
	// evaluated: both are statements about an entry's position in an ordering, not about whether
	// two blobs happen to differ.
	TierValue Tier = "value"

	// TierVersion compares a version extracted from the cached value against a database column.
	//
	// The flagship tier for a foreign deployment: it costs the application one field in whatever
	// it already caches, and it buys every guarantee except the ones that need Cachet's own clock.
	TierVersion Tier = "version"

	// TierCachet is a Cachet deployment, where the entry carries an HLC fill version.
	TierCachet Tier = "cachet"
)

// Valid reports whether the tier is one this package knows.
func (t Tier) Valid() bool {
	switch t {
	case TierValue, TierVersion, TierCachet:
		return true
	default:
		return false
	}
}

// ComparesVersions reports whether this tier can order an entry against the database.
//
// SESSION and BOUNDED both rest on that ordering, so a tier that cannot do it must not report on
// them — not as "zero violations", which reads as a clean bill of health, but as not measured.
func (t Tier) ComparesVersions() bool { return t == TierVersion || t == TierCachet }

// ParseTier reads a tier name from configuration.
func ParseTier(s string) (Tier, error) {
	t := Tier(s)
	if !t.Valid() {
		return "", fmt.Errorf("sextant: unknown tier %q; want value, version or cachet", s)
	}
	return t, nil
}

// Difference is the result of comparing one cache entry against the origin.
//
// It replaces the pair of version numbers the verifier used to carry, because two uint64s is an
// assumption that versions exist. A value-tier comparison produces a Difference with no versions
// in it, and everything downstream that needs an ordering checks VersionsKnown rather than
// treating a zero as a version.
type Difference struct {
	// Behind reports that the entry does not reflect the origin's current state.
	Behind bool

	// FillVersion is the database state the entry was filled from; DBVersion is the row's version
	// now. Both are meaningless unless VersionsKnown.
	FillVersion   uint64
	DBVersion     uint64
	VersionsKnown bool
}

// VersionDifference builds a difference from two versions.
//
// An entry at or ahead of the database is not behind — including the ahead case, which happens
// legitimately when the verifier read the cache after a write landed and the database before it.
// Reporting that would make the verifier's own read ordering look like a cache bug.
func VersionDifference(fillVersion, dbVersion uint64) Difference {
	return Difference{
		Behind:        fillVersion < dbVersion,
		FillVersion:   fillVersion,
		DBVersion:     dbVersion,
		VersionsKnown: true,
	}
}

// ValueDifference builds a difference from a value comparison.
//
// Equal values mean the entry is not behind. Unequal values mean it is — or that the projection
// compared is not the one the application caches, which is a configuration error that presents as
// a permanent violation and is meant to.
func ValueDifference(equal bool) Difference {
	return Difference{Behind: !equal}
}
