//go:build integration

package cache_test

import (
	"context"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
)

// A cached read and an uncached read of the same row must be indistinguishable. Anything less means
// the answer a caller gets depends on whether the cache happened to be warm — which is a
// correctness bug wearing a performance costume, and exactly the class of failure Cachet exists to
// eliminate.

func TestAnEntryRoundTripsEveryField(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	// The row is opaque to this layer: these bytes stand in for whatever the schema encoded. What
	// is asserted is that they come back unchanged, because a cache hit must return the same
	// record as a miss — and every column now lives in here.
	want := cache.Entry{
		RowVersion:  4242,
		FillVersion: 9999,
		Row:         []byte("\x01\x00payload\xff\x00binary"),
	}
	if _, err := c.Fill(ctx, "entities:roundtrip", want); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	got, hit, err := c.Get(ctx, "entities:roundtrip")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Fatal("Get reported a miss for a key that was just filled")
	}

	if got.RowVersion != want.RowVersion || got.FillVersion != want.FillVersion {
		t.Errorf("versions = (%d,%d), want (%d,%d)", got.RowVersion, got.FillVersion, want.RowVersion, want.FillVersion)
	}
	if string(got.Row) != string(want.Row) {
		t.Errorf("Payload = %q, want %q", got.Row, want.Row)
	}
}

func TestZeroValuedFieldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	// An empty row is a legitimate value, not an absence — a table can encode to no bytes only if
	// it has no columns, but the distinction between "stored and empty" and "not stored" is the
	// same one that made zero-valued columns read back wrong before the row was carried whole.
	want := cache.Entry{RowVersion: 5, FillVersion: 5, Row: []byte{}}
	if _, err := c.Fill(ctx, "entities:roundtrip-zero", want); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	got, hit, err := c.Get(ctx, "entities:roundtrip-zero")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Fatal("Get reported a miss")
	}
	if got.Negative {
		t.Error("an entry with an empty row came back as a negative entry; 'stored and empty' is not 'row does not exist'")
	}
	if len(got.Row) != 0 {
		t.Errorf("empty row came back as %q", got.Row)
	}
}

func TestNegativeEntriesCarryNoRowFields(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	// A negative entry is a cached absence: there is no row, so there are no row fields to carry.
	want := cache.Entry{RowVersion: 11, FillVersion: 11, Negative: true}
	if _, err := c.Fill(ctx, "entities:roundtrip-negative", want); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	got, hit, err := c.Get(ctx, "entities:roundtrip-negative")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Fatal("Get reported a miss for a negative entry; the absence itself is the cached fact")
	}
	if !got.Negative {
		t.Error("the entry lost its negative flag")
	}
}
