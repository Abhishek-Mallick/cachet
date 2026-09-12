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

	want := cache.Entry{
		RowVersion:  4242,
		FillVersion: 9999,
		TenantID:    77,
		Status:      3,
		Payload:     []byte("payload"),
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

	if got.TenantID != want.TenantID {
		t.Errorf("TenantID = %d, want %d; a cache hit returns a different row than a miss", got.TenantID, want.TenantID)
	}
	if got.Status != want.Status {
		t.Errorf("Status = %d, want %d; a cache hit returns a different row than a miss", got.Status, want.Status)
	}
	if got.RowVersion != want.RowVersion || got.FillVersion != want.FillVersion {
		t.Errorf("versions = (%d,%d), want (%d,%d)", got.RowVersion, got.FillVersion, want.RowVersion, want.FillVersion)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("Payload = %q, want %q", got.Payload, want.Payload)
	}
}

func TestZeroValuedFieldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	// Status 0 is a legitimate value, not an absence. If the encoding could not tell "status is
	// zero" from "status was not stored", every row in the default state would read back wrong.
	want := cache.Entry{RowVersion: 5, FillVersion: 5, TenantID: 0, Status: 0, Payload: []byte("z")}
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
	if got.TenantID != 0 || got.Status != 0 {
		t.Errorf("zero-valued fields came back as (%d,%d), want (0,0)", got.TenantID, got.Status)
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
