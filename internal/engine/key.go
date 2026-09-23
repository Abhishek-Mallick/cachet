package engine

import (
	"fmt"
	"strconv"
	"strings"
)

// Key identifies one cacheable row.
//
// It is the unit of routing AND of cache identity, which is why parsing is strict. A key the engine
// half-understands is worse than one it refuses: it would route somewhere deterministic and then be
// cached under an identity nothing else can reproduce, producing staleness that looks like a cache
// bug and is actually a parsing bug.
type Key struct {
	Table string
	ID    uint64
}

// ParseKey parses a key of the form "<table>:<id>" belonging to a named table.
//
// The table is a parameter rather than a constant because Cachet no longer has a built-in table:
// what it serves is declared. Restricting the key to that one table is still deliberate rather
// than a shortcut — routing and cache identity are both derived from the key, so accepting an
// undeclared table would mean caching rows under an identity that no invalidation path knows how
// to produce.
func ParseKey(cachedTable, s string) (Key, error) {
	table, rawID, ok := strings.Cut(s, ":")
	if !ok {
		return Key{}, fmt.Errorf("engine: key %q must be <table>:<id>", s)
	}
	if table != cachedTable {
		return Key{}, fmt.Errorf("engine: unknown table %q in key %q", table, s)
	}
	if rawID == "" {
		return Key{}, fmt.Errorf("engine: key %q has no id", s)
	}

	id, err := strconv.ParseUint(rawID, 10, 64)
	if err != nil {
		return Key{}, fmt.Errorf("engine: key %q has an invalid id: %w", s, err)
	}
	return Key{Table: table, ID: id}, nil
}

// String returns the canonical form of the key.
func (k Key) String() string {
	return k.Table + ":" + strconv.FormatUint(k.ID, 10)
}
