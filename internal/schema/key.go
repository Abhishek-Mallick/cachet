package schema

import (
	"fmt"
	"strconv"
	"strings"
)

// Key names one cacheable row.
//
// It is the unit of routing AND of cache identity, which is why the grammar is strict and why it
// must be injective: two primary keys that produce one key mean one row's value served for another
// row, which no test downstream would catch as anything but inexplicable staleness.
//
//	<table>:<value>[:<value>…]
//
// Values are percent-escaped so that ':' and '%' never appear raw, which makes splitting
// unambiguous. A single integer primary key escapes to itself, so "entities:123" is byte-identical
// to the key Cachet has always produced — and the hash ring therefore places existing entries
// exactly where it did before.
type Key struct {
	Table string

	// Values are the primary key columns in declared order, decoded.
	Values []string
}

// String renders the key.
func (k Key) String() string {
	var b strings.Builder
	b.WriteString(k.Table)
	for _, v := range k.Values {
		b.WriteByte(':')
		writeEscaped(&b, v)
	}
	return b.String()
}

// Key builds a key from primary key values, in declared order.
func (d *Descriptor) Key(values ...any) (Key, error) {
	if len(values) != len(d.PrimaryKey) {
		return Key{}, fmt.Errorf("schema: %s has %d primary key column(s), got %d value(s)",
			d.Name, len(d.PrimaryKey), len(values))
	}

	out := make([]string, len(values))
	for i, v := range values {
		s, err := canonical(d.PrimaryKey[i], v)
		if err != nil {
			return Key{}, err
		}
		out[i] = s
	}
	return Key{Table: d.Name, Values: out}, nil
}

// canonical renders one primary key value.
//
// Integers render in plain decimal with no padding, which is what keeps a single-integer key
// identical to the historical format. Everything else is carried as its bytes.
func canonical(col *Column, v any) (string, error) {
	switch val := v.(type) {
	case uint64:
		return strconv.FormatUint(val, 10), nil
	case uint32:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case int:
		return strconv.Itoa(val), nil
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	default:
		return "", fmt.Errorf("schema: %s: cannot use %T as a primary key value", col.Name, v)
	}
}

// KeyOf builds a key for a table from already-ordered primary key values.
//
// Descriptor-free, because the CDC tailer knows a table's name and its key columns' positions from
// the binlog event itself and may be following several tables at once. The grammar is the same one
// Descriptor.Key uses, which is the requirement: a tailer and an engine must agree on what a row is
// called, or an invalidation lands under a key nobody reads.
func KeyOf(table string, values ...any) (Key, error) {
	if !validIdentifier(table) {
		return Key{}, fmt.Errorf("schema: %q is not a plain SQL identifier", table)
	}
	if len(values) == 0 {
		return Key{}, fmt.Errorf("schema: key for %q has no values", table)
	}
	out := make([]string, len(values))
	for i, v := range values {
		s, err := canonical(&Column{Name: "key"}, v)
		if err != nil {
			return Key{}, err
		}
		out[i] = s
	}
	return Key{Table: table, Values: out}, nil
}

// ParseKey parses a key without needing a descriptor.
//
// Deliberately descriptor-free: the CDC tailer and Sextant both recover a primary key from a key
// string, and requiring a descriptor there would make key parsing depend on configuration those
// components may not share.
func ParseKey(s string) (Key, error) {
	table, rest, ok := strings.Cut(s, ":")
	if !ok {
		return Key{}, fmt.Errorf("schema: key %q must be <table>:<value>[:<value>…]", s)
	}
	if !validIdentifier(table) {
		return Key{}, fmt.Errorf("schema: key %q has an invalid table name", s)
	}

	// An empty segment is a legal value: '' is a permitted PRIMARY KEY value for a VARCHAR column.
	// Parsing is descriptor-free and cannot know a column's type, so rejecting an empty value for
	// an integer key belongs to the descriptor path rather than to the grammar.
	parts := strings.Split(rest, ":")
	values := make([]string, 0, len(parts))
	for _, p := range parts {
		v, err := unescape(p)
		if err != nil {
			return Key{}, fmt.Errorf("schema: key %q: %w", s, err)
		}
		values = append(values, v)
	}
	return Key{Table: table, Values: values}, nil
}

const hexDigits = "0123456789ABCDEF"

// writeEscaped escapes the separator, the escape character itself, and anything unprintable.
//
// The first two are required for injectivity. The third is operational: keys are printed by
// cachetctl and appear in logs, and a raw control byte there is a hazard for whoever is reading.
func writeEscaped(b *strings.Builder, s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ':' || c == '%' || c < 0x20 || c == 0x7f {
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0x0f])
			continue
		}
		b.WriteByte(c)
	}
}

func unescape(s string) (string, error) {
	if !strings.Contains(s, "%") {
		return s, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			b.WriteByte(s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", fmt.Errorf("truncated escape at byte %d", i)
		}
		hi, err := hexVal(s[i+1])
		if err != nil {
			return "", err
		}
		lo, err := hexVal(s[i+2])
		if err != nil {
			return "", err
		}
		b.WriteByte(hi<<4 | lo)
		i += 2
	}
	return b.String(), nil
}

func hexVal(c byte) (byte, error) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', nil
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, nil
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, nil
	default:
		return 0, fmt.Errorf("invalid escape digit %q", c)
	}
}
