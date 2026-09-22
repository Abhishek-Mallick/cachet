package schema_test

import (
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// A row codec is exactly the kind of code a fuzzer finds bugs in: length prefixes, a bitmap, and a
// trailing-bytes check. Round-tripping arbitrary bytes is the property that matters, because a
// payload column holds whatever the application put there.
func FuzzRowRoundTrip(f *testing.F) {
	f.Add("alice", []byte{0, 1, 2}, int64(0), true)
	f.Add("", []byte{}, int64(-1), false)
	f.Add("🎯", []byte{0xff, 0x00, 0xfe}, int64(9223372036854775807), true)

	d := nullable()
	f.Fuzz(func(t *testing.T, name string, blob []byte, count int64, isNull bool) {
		row := []schema.Value{
			schema.Uint(1),
			schema.Str(name),
			schema.Null(),
			schema.Bin(blob),
			schema.Int(count),
			schema.Uint(2),
		}
		if isNull {
			row[1] = schema.Null()
			row[4] = schema.Null()
		}

		enc, err := d.EncodeRow(row)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		back, err := d.DecodeRow(enc)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		for i := range row {
			if !row[i].Equal(back[i]) {
				t.Fatalf("column %d: %+v → %+v", i, row[i], back[i])
			}
		}
	})
}

// Arbitrary bytes must never decode into a row. The cache is shared state; a corrupt or hostile
// entry must be refused rather than served.
func FuzzDecodeRowRejectsGarbage(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add([]byte{1, 0, 0xff, 0xff, 0xff})

	d := nullable()
	f.Fuzz(func(t *testing.T, b []byte) {
		values, err := d.DecodeRow(b)
		if err != nil {
			return // refusing is the expected outcome
		}
		// If it decoded, it must be a complete, well-formed row — never a partial one.
		if len(values) != 6 {
			t.Fatalf("decoded %d columns from %d bytes", len(values), len(b))
		}
		if _, err := d.EncodeRow(values); err != nil {
			t.Fatalf("decoded a row that cannot be re-encoded: %v", err)
		}
	})
}

// The key grammar has the same shape of risk: an escape parser and a separator.
func FuzzKeyRoundTrip(f *testing.F) {
	f.Add("alice", "bob")
	f.Add("a:b", "%")
	f.Add("", "")

	d, err := schema.NewDescriptor(schema.TableConfig{
		Name: "t", PrimaryKey: []string{"a", "b"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "a", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "b", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "version", Type: schema.Uint64},
		},
	})
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, a, b string) {
		k, err := d.Key(a, b)
		if err != nil {
			t.Fatalf("Key: %v", err)
		}
		back, err := schema.ParseKey(k.String())
		if err != nil {
			t.Fatalf("ParseKey(%q): %v", k.String(), err)
		}
		if len(back.Values) != 2 || back.Values[0] != a || back.Values[1] != b {
			t.Fatalf("(%q,%q) → %q → %q", a, b, k.String(), back.Values)
		}
	})
}
