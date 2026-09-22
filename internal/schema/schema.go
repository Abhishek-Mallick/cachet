// Package schema describes the one thing Cachet needs to know about a user's table.
//
// Everything below the API — routing, cache identity, invalidation, the proxy's classifier — is
// derived from a Descriptor, and a Descriptor is built once at boot from configuration that has
// been validated and then checked against INFORMATION_SCHEMA. Nothing here reads a request.
//
// The stance is the same one the rest of the codebase takes about SQL it does not understand:
// refusing a table costs a deployment an error message at boot, and misunderstanding one costs
// correctness at 3am. So this package refuses a great deal.
package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// Type is a column type Cachet can round-trip through a cache entry.
//
// A closed set, deliberately. A column whose type is not here is not cacheable, which the proxy
// already knows how to express — it refuses to serve a statement naming a column it cannot
// reconstruct.
type Type string

const (
	Uint8  Type = "uint8"
	Uint32 Type = "uint32"
	Uint64 Type = "uint64"
	Int64  Type = "int64"

	// String and Bytes differ only in how a value is compared and rendered; both are carried as
	// raw bytes so what comes back is byte-identical to what MySQL sent.
	String Type = "string"
	Bytes  Type = "bytes"

	// Text carries DECIMAL, DATETIME, TIMESTAMP and JSON as the text MySQL produced, rather than
	// parsing and re-rendering them. A round trip through time.Time loses the distinction between
	// what was stored and what a driver chose to format, and DECIMAL through float64 loses money.
	Text Type = "text"
)

var integerTypes = map[Type]bool{Uint8: true, Uint32: true, Uint64: true, Int64: true}

func (t Type) valid() bool {
	switch t {
	case Uint8, Uint32, Uint64, Int64, String, Bytes, Text:
		return true
	default:
		return false
	}
}

// ColumnConfig is one declared column.
type ColumnConfig struct {
	Name     string `koanf:"name"`
	Type     Type   `koanf:"type"`
	Nullable bool   `koanf:"nullable"`

	// Collation matters only for a string column in the primary key. See NewDescriptor.
	Collation string `koanf:"collation"`
}

// TableConfig is a declared table.
type TableConfig struct {
	Name          string         `koanf:"name"`
	PrimaryKey    []string       `koanf:"primary_key"`
	VersionColumn string         `koanf:"version_column"`
	Columns       []ColumnConfig `koanf:"columns"`
}

// Column is a validated column, with its position fixed.
type Column struct {
	Name     string
	Type     Type
	Nullable bool

	// Index is the column's position in the encoded row. It is part of the fingerprint, so a
	// reordering invalidates every entry rather than decoding old bytes into new positions.
	Index int
}

// Quoted renders the column for SQL.
func (c *Column) Quoted() string { return quote(c.Name) }

// Descriptor is everything the rest of the engine needs about one table.
type Descriptor struct {
	Name          string
	Columns       []Column
	PrimaryKey    []*Column
	VersionColumn *Column

	// Fingerprint identifies the SHAPE of an encoded row.
	//
	// It is stored beside every cache entry so that a descriptor change makes old entries read as
	// a miss instead of decoding into the wrong shape — no flush, no downtime, and two engines
	// mid-rolling-deploy simply miss each other rather than corrupt anything.
	//
	// It deliberately does NOT appear in the cache key: that would remap the hash ring on every
	// deploy and stampede the origin.
	Fingerprint string

	byName map[string]*Column
}

// Column resolves a column by name, or nil.
func (d *Descriptor) Column(name string) *Column { return d.byName[name] }

// QuotedName renders the table for SQL.
func (d *Descriptor) QuotedName() string { return quote(d.Name) }

// identifierRe is what may reach a SQL statement.
//
// Narrower than MySQL accepts, on purpose: a table this refuses can be renamed, and a table it
// mis-handles cannot be un-corrupted.
var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,63}$`)

func validIdentifier(s string) bool { return identifierRe.MatchString(s) }

// quote renders a validated identifier for SQL.
//
// The doubling is belt and braces — validIdentifier has already rejected a backtick — because this
// is the last function between a configuration value and a statement.
func quote(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// NewDescriptor validates a declaration and builds the descriptor.
func NewDescriptor(cfg TableConfig) (*Descriptor, error) {
	if !validIdentifier(cfg.Name) {
		return nil, fmt.Errorf("schema: table name %q is not a plain SQL identifier", cfg.Name)
	}
	if len(cfg.Columns) == 0 {
		return nil, fmt.Errorf("schema: table %q declares no columns", cfg.Name)
	}

	d := &Descriptor{Name: cfg.Name, byName: make(map[string]*Column, len(cfg.Columns))}
	for i, c := range cfg.Columns {
		if !validIdentifier(c.Name) {
			return nil, fmt.Errorf("schema: %s: column name %q is not a plain SQL identifier", cfg.Name, c.Name)
		}
		if !c.Type.valid() {
			return nil, fmt.Errorf("schema: %s.%s: type %q is not one Cachet can round-trip", cfg.Name, c.Name, c.Type)
		}
		if _, dup := d.byName[c.Name]; dup {
			return nil, fmt.Errorf("schema: %s: column %q declared twice", cfg.Name, c.Name)
		}
		d.Columns = append(d.Columns, Column{Name: c.Name, Type: c.Type, Nullable: c.Nullable, Index: i})
	}
	for i := range d.Columns {
		d.byName[d.Columns[i].Name] = &d.Columns[i]
	}

	if len(cfg.PrimaryKey) == 0 {
		return nil, fmt.Errorf("schema: %s declares no primary key; it is how a row is named", cfg.Name)
	}
	for _, name := range cfg.PrimaryKey {
		col := d.byName[name]
		if col == nil {
			return nil, fmt.Errorf("schema: %s: primary key column %q is not declared", cfg.Name, name)
		}
		if col.Nullable {
			return nil, fmt.Errorf("schema: %s.%s is nullable and cannot be part of a primary key", cfg.Name, name)
		}
		if err := checkKeyCollation(cfg, col); err != nil {
			return nil, err
		}
		d.PrimaryKey = append(d.PrimaryKey, col)
	}

	ver := d.byName[cfg.VersionColumn]
	switch {
	case cfg.VersionColumn == "":
		return nil, fmt.Errorf("schema: %s declares no version column; every invalidation is a compare-and-set against it", cfg.Name)
	case ver == nil:
		return nil, fmt.Errorf("schema: %s: version column %q is not declared", cfg.Name, cfg.VersionColumn)
	case !integerTypes[ver.Type]:
		return nil, fmt.Errorf("schema: %s.%s is %q; a version column must be an integer because every compare-and-set orders on it",
			cfg.Name, ver.Name, ver.Type)
	case ver.Nullable:
		return nil, fmt.Errorf("schema: %s.%s is nullable; a NULL version cannot be compared", cfg.Name, ver.Name)
	}
	d.VersionColumn = ver

	d.Fingerprint = fingerprint(d)
	return d, nil
}

// checkKeyCollation refuses a primary key whose collation would let two cache keys name one row.
//
// Under a case- or accent-insensitive collation MySQL treats 'Ann' and 'ann' as the SAME row, while
// this package would derive two distinct cache keys from them. A write under one and a cached entry
// under the other means an invalidation that silently misses — a stale read with no failure
// anywhere to report it. There is no way to detect this at runtime, so it is refused at boot.
func checkKeyCollation(cfg TableConfig, col *Column) error {
	if col.Type != String && col.Type != Text {
		return nil
	}
	var declared string
	for _, c := range cfg.Columns {
		if c.Name == col.Name {
			declared = strings.ToLower(c.Collation)
		}
	}
	if declared == "" {
		return fmt.Errorf("schema: %s.%s is a string primary key and declares no collation; "+
			"Cachet needs to know it is case- and accent-sensitive, because otherwise two cache keys can name one row",
			cfg.Name, col.Name)
	}
	if strings.HasSuffix(declared, "_ci") || strings.HasSuffix(declared, "_ai") ||
		strings.Contains(declared, "_ai_") || strings.Contains(declared, "_ci_") {
		return fmt.Errorf("schema: %s.%s uses collation %q: MySQL treats values differing only by "+
			"case or accent as the same row, but they produce different cache keys — so an invalidation "+
			"would silently miss. Use a _bin or _as_cs collation for a cached primary key",
			cfg.Name, col.Name, declared)
	}
	return nil
}

// fingerprint hashes everything that changes how a row encodes or how a key is built.
func fingerprint(d *Descriptor) string {
	var b strings.Builder
	b.WriteString("cachet.schema.v1\n")
	b.WriteString(d.Name)
	b.WriteByte('\n')
	for _, c := range d.Columns {
		fmt.Fprintf(&b, "%d:%s:%s:%t\n", c.Index, c.Name, c.Type, c.Nullable)
	}
	b.WriteString("pk\n")
	for _, c := range d.PrimaryKey {
		b.WriteString(c.Name)
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	// Sixteen hex characters: this is stored on every cache entry, and a collision would need a
	// deliberate construction rather than an accident.
	return hex.EncodeToString(sum[:8])
}
