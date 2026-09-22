package schema

import (
	"encoding/binary"
	"fmt"
)

// Value is one column of one row.
//
// IsNull is carried separately rather than encoded as an absent Bytes, because SQL NULL and a
// zero-length value are different facts. A third fact — the row does not exist — is recorded by the
// cache as a negative entry and never reaches this type. Three absences, all distinct.
type Value struct {
	IsNull bool

	// Bytes is the canonical representation: decimal text for integers, the raw bytes for
	// everything else. Carrying MySQL's own bytes rather than a parsed value is what makes a cache
	// hit byte-identical to a database read, which the proxy relies on to answer over the wire.
	Bytes []byte
}

// Null builds a NULL value.
func Null() Value { return Value{IsNull: true} }

// Str builds a string value.
func Str(s string) Value { return Value{Bytes: []byte(s)} }

// Bin builds a byte value.
func Bin(b []byte) Value { return Value{Bytes: b} }

// Uint builds an unsigned integer value.
func Uint(u uint64) Value { return Value{Bytes: appendUint(nil, u)} }

// Int builds a signed integer value.
func Int(i int64) Value { return Value{Bytes: appendInt(nil, i)} }

// Equal compares two values, treating NULL as equal only to NULL.
func (v Value) Equal(o Value) bool {
	if v.IsNull || o.IsNull {
		return v.IsNull && o.IsNull
	}
	return string(v.Bytes) == string(o.Bytes)
}

// String renders the value for logs and for the CLI.
func (v Value) String() string {
	if v.IsNull {
		return "NULL"
	}
	return string(v.Bytes)
}

func appendUint(dst []byte, u uint64) []byte {
	var buf [20]byte
	return append(dst, appendUintTo(buf[:0], u)...)
}

func appendUintTo(dst []byte, u uint64) []byte {
	if u == 0 {
		return append(dst, '0')
	}
	var tmp [20]byte
	i := len(tmp)
	for u > 0 {
		i--
		tmp[i] = byte('0' + u%10)
		u /= 10
	}
	return append(dst, tmp[i:]...)
}

func appendInt(dst []byte, i int64) []byte {
	if i < 0 {
		dst = append(dst, '-')
		// Negated as unsigned so math.MinInt64 does not overflow.
		return appendUintTo(dst, -uint64(i)) //nolint:gosec // deliberate: two's-complement negation of MinInt64
	}
	return appendUintTo(dst, uint64(i))
}

// rowFormat is the first byte of every encoded row.
//
// The fingerprint beside the entry already rejects a shape change, so this exists only for a change
// to the ENCODING itself — a new format byte lets an old decoder refuse rather than misread.
const rowFormat = 1

// EncodeRow encodes a row, positionally, with an explicit null bitmap.
//
// Positional rather than named: names are already fixed by the descriptor, and the fingerprint
// guarantees a decoder shares that descriptor, so repeating a name per column per row would be paid
// on every fill and every hit for nothing.
func (d *Descriptor) EncodeRow(values []Value) ([]byte, error) {
	if len(values) != len(d.Columns) {
		return nil, fmt.Errorf("schema: %s has %d columns, got %d values", d.Name, len(d.Columns), len(values))
	}

	nullBytes := (len(d.Columns) + 7) / 8
	size := 1 + nullBytes
	for _, v := range values {
		if !v.IsNull {
			size += binary.MaxVarintLen64 + len(v.Bytes)
		}
	}

	out := make([]byte, 0, size)
	out = append(out, rowFormat)

	nulls := make([]byte, nullBytes)
	for i, v := range values {
		if !v.IsNull {
			continue
		}
		if !d.Columns[i].Nullable {
			return nil, fmt.Errorf("schema: %s.%s is NOT NULL but the value is NULL; the declaration no longer matches the database",
				d.Name, d.Columns[i].Name)
		}
		nulls[i/8] |= 1 << (i % 8)
	}
	out = append(out, nulls...)

	for _, v := range values {
		if v.IsNull {
			continue
		}
		out = binary.AppendUvarint(out, uint64(len(v.Bytes)))
		out = append(out, v.Bytes...)
	}
	return out, nil
}

// DecodeRow decodes a row.
//
// Every failure is an error and never a partial row: a short read that produced three of six
// columns would be served as though it were the row, which is a wrong answer rather than a missing
// one.
func (d *Descriptor) DecodeRow(b []byte) ([]Value, error) {
	nullBytes := (len(d.Columns) + 7) / 8
	if len(b) < 1+nullBytes {
		return nil, fmt.Errorf("schema: %s: encoded row is %d bytes, too short for %d columns",
			d.Name, len(b), len(d.Columns))
	}
	if b[0] != rowFormat {
		return nil, fmt.Errorf("schema: %s: encoded row has format %d, this build writes %d", d.Name, b[0], rowFormat)
	}

	nulls := b[1 : 1+nullBytes]
	rest := b[1+nullBytes:]

	values := make([]Value, len(d.Columns))
	for i := range d.Columns {
		if nulls[i/8]&(1<<(i%8)) != 0 {
			// Symmetric with EncodeRow, and it has to be: the cache is shared state, so an entry
			// may be corrupt or hostile. Without this a crafted bitmap yields a row whose NOT NULL
			// primary key is NULL — a wrong answer rather than a refused one. Found by fuzzing.
			if !d.Columns[i].Nullable {
				return nil, fmt.Errorf("schema: %s.%s is NOT NULL but the encoded row marks it NULL",
					d.Name, d.Columns[i].Name)
			}
			values[i] = Value{IsNull: true}
			continue
		}
		n, used := binary.Uvarint(rest)
		if used <= 0 {
			return nil, fmt.Errorf("schema: %s.%s: truncated length", d.Name, d.Columns[i].Name)
		}
		rest = rest[used:]
		if uint64(len(rest)) < n {
			return nil, fmt.Errorf("schema: %s.%s: value claims %d bytes, %d remain", d.Name, d.Columns[i].Name, n, len(rest))
		}
		// Copied rather than aliased: the caller may hold the row after the buffer is reused.
		values[i] = Value{Bytes: append([]byte(nil), rest[:n]...)}
		rest = rest[n:]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("schema: %s: %d bytes left after %d columns", d.Name, len(rest), len(d.Columns))
	}
	return values, nil
}
