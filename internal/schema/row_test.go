package schema_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

func nullable() *schema.Descriptor {
	d, err := schema.NewDescriptor(schema.TableConfig{
		Name: "t", PrimaryKey: []string{"id"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: schema.Uint64},
			{Name: "name", Type: schema.String, Nullable: true},
			{Name: "note", Type: schema.Text, Nullable: true},
			{Name: "blob", Type: schema.Bytes, Nullable: true},
			{Name: "count", Type: schema.Int64, Nullable: true},
			{Name: "version", Type: schema.Uint64},
		},
	})
	if err != nil {
		panic(err)
	}
	return d
}

// The distinction the whole codec exists to preserve.
//
// SQL NULL and a zero value are different facts, and "this column is NULL" is a different fact
// again from "this row does not exist" — which the cache records separately as a negative entry.
// Collapsing any pair of those means answering a question the caller did not ask.
func TestNullIsNotTheZeroValue(t *testing.T) {
	t.Parallel()

	d := nullable()
	withNull := []schema.Value{
		schema.Uint(1), schema.Null(), schema.Null(), schema.Null(), schema.Null(), schema.Uint(9),
	}
	withZero := []schema.Value{
		schema.Uint(1), schema.Str(""), schema.Str(""), schema.Bin([]byte{}), schema.Int(0), schema.Uint(9),
	}

	a, err := d.EncodeRow(withNull)
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.EncodeRow(withZero)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a, b) {
		t.Fatal("a row of NULLs encoded identically to a row of zero values")
	}

	back, err := d.DecodeRow(a)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		if !back[i].IsNull {
			t.Errorf("column %d came back as %v, want NULL", i, back[i])
		}
	}
	back, _ = d.DecodeRow(b)
	for i := 1; i <= 4; i++ {
		if back[i].IsNull {
			t.Errorf("column %d came back NULL, want a zero value", i)
		}
	}
}

func TestARowRoundTrips(t *testing.T) {
	t.Parallel()

	d := nullable()
	rows := [][]schema.Value{
		{schema.Uint(1), schema.Str("alice"), schema.Str("2026-09-23 10:00:00"), schema.Bin([]byte{0, 1, 2, 0xff}), schema.Int(-5), schema.Uint(7)},
		{schema.Uint(18446744073709551615), schema.Null(), schema.Str(""), schema.Bin(nil), schema.Int(0), schema.Uint(1)},
		{schema.Uint(2), schema.Str(strings.Repeat("x", 70000)), schema.Null(), schema.Null(), schema.Null(), schema.Uint(3)},
		{schema.Uint(3), schema.Str("emoji 🎯 and ':' and '%'"), schema.Null(), schema.Null(), schema.Int(-9223372036854775808), schema.Uint(4)},
	}
	for i, row := range rows {
		enc, err := d.EncodeRow(row)
		if err != nil {
			t.Fatalf("row %d: encode: %v", i, err)
		}
		back, err := d.DecodeRow(enc)
		if err != nil {
			t.Fatalf("row %d: decode: %v", i, err)
		}
		if len(back) != len(row) {
			t.Fatalf("row %d: %d columns back, want %d", i, len(back), len(row))
		}
		for j := range row {
			if !row[j].Equal(back[j]) {
				t.Errorf("row %d column %d: %+v → %+v", i, j, row[j], back[j])
			}
		}
	}
}

func TestAWrongShapedRowIsRefused(t *testing.T) {
	t.Parallel()

	d := nullable()
	if _, err := d.EncodeRow([]schema.Value{schema.Uint(1)}); err == nil {
		t.Error("encoded a row with too few columns")
	}
	// A NULL in a column declared NOT NULL is a declaration that no longer matches the database.
	if _, err := d.EncodeRow([]schema.Value{
		schema.Null(), schema.Null(), schema.Null(), schema.Null(), schema.Null(), schema.Uint(1),
	}); err == nil {
		t.Error("encoded a NULL into a NOT NULL column")
	}
}

// Truncated or corrupt bytes must be an error, never a partially decoded row. A short read that
// produced three of six columns would be served as though it were the row.
func TestCorruptBytesAreRefusedRatherThanPartiallyDecoded(t *testing.T) {
	t.Parallel()

	d := nullable()
	good, err := d.EncodeRow([]schema.Value{
		schema.Uint(1), schema.Str("alice"), schema.Str("x"), schema.Bin([]byte{1}), schema.Int(2), schema.Uint(3),
	})
	if err != nil {
		t.Fatal(err)
	}

	for n := 0; n < len(good); n++ {
		if _, err := d.DecodeRow(good[:n]); err == nil {
			t.Errorf("decoded a row truncated to %d of %d bytes", n, len(good))
		}
	}
	if _, err := d.DecodeRow(nil); err == nil {
		t.Error("decoded nil bytes")
	}
}
