// Package consistency defines Cachet's consistency levels and session watermarks.
//
// It is shared by the engine, the client SDK and Sextant, and it is public because all three must
// agree on what a level means down to the edge cases. If the SDK and the engine held separate
// definitions, the guarantee would be whatever the two happened to have in common — which is how a
// consistency model becomes prose rather than a contract.
//
// The normative definitions live in CONSISTENCY.md. This package is that document, executable.
package consistency

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Level is a per-request consistency guarantee.
type Level int

const (
	// Session is read-own-writes plus monotonic reads, scoped to a session token. It is the
	// default, and it is the zero value so that a caller who forgets to choose gets the documented
	// default rather than the weakest option.
	Session Level = iota

	// Strong bypasses the cache entirely.
	Strong

	// Bounded is Session plus a freshness floor measured from commit time.
	Bounded

	// Eventual is best effort: no read-own-writes, no monotonic reads.
	Eventual
)

var levelNames = map[Level]string{
	Session:  "SESSION",
	Strong:   "STRONG",
	Bounded:  "BOUNDED",
	Eventual: "EVENTUAL",
}

// String returns the level's canonical name.
func (l Level) String() string {
	if n, ok := levelNames[l]; ok {
		return n
	}
	return fmt.Sprintf("Level(%d)", int(l))
}

// ParseLevel resolves a level name, case-insensitively.
//
// It rejects unknown names rather than defaulting, because a typo in a configuration file must fail
// at boot instead of silently selecting a weaker guarantee than the operator asked for.
func ParseLevel(s string) (Level, error) {
	upper := strings.ToUpper(strings.TrimSpace(s))
	for lv, name := range levelNames {
		if name == upper {
			return lv, nil
		}
	}
	return 0, fmt.Errorf("consistency: unknown level %q (want STRONG, SESSION, BOUNDED or EVENTUAL)", s)
}

// BypassesCache reports whether reads at this level must go straight to the database.
func (l Level) BypassesCache() bool { return l == Strong }

// RequiresWatermark reports whether this level enforces the session watermark on a cache entry.
//
// Eventual deliberately does not: it is the control group that makes the cost of the other levels
// visible in the benchmark table.
func (l Level) RequiresWatermark() bool { return l == Session || l == Bounded }

// Errors returned by Requirement.Validate.
var (
	// ErrMissingStalenessBound reports a BOUNDED request with no bound. There is no default: a
	// bound the caller did not choose is a guarantee the caller cannot rely on.
	ErrMissingStalenessBound = errors.New("consistency: BOUNDED requires a staleness bound")

	// ErrUnexpectedStalenessBound reports a bound on a level that has no use for one. Accepting and
	// ignoring it would let a caller believe they had asked for a freshness guarantee they are not
	// getting.
	ErrUnexpectedStalenessBound = errors.New("consistency: staleness bound is only valid with BOUNDED")
)

// Requirement is a fully specified request-level guarantee.
type Requirement struct {
	Level Level

	// StalenessBound is meaningful only for Bounded, and is measured from COMMIT time — not from
	// read time. See CONSISTENCY.md §3.3.
	StalenessBound time.Duration
}

// Validate reports whether the requirement is coherent. Callers must validate before serving:
// an incoherent requirement is a caller bug, and answering it anyway means guessing which half the
// caller meant.
func (r Requirement) Validate() error {
	if _, ok := levelNames[r.Level]; !ok {
		return fmt.Errorf("consistency: unknown level %d", int(r.Level))
	}
	if r.Level == Bounded {
		if r.StalenessBound == 0 {
			return ErrMissingStalenessBound
		}
		if r.StalenessBound < 0 {
			return fmt.Errorf("consistency: negative staleness bound %s", r.StalenessBound)
		}
		return nil
	}
	if r.StalenessBound != 0 {
		return fmt.Errorf("%w (got %s with %s)", ErrUnexpectedStalenessBound, r.StalenessBound, r.Level)
	}
	return nil
}
