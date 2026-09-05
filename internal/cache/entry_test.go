package cache_test

import (
	"bytes"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
)

func TestEntryRoundTrips(t *testing.T) {
	t.Parallel()

	want := cache.Entry{RowVersion: 1234567890123, FillVersion: 9876543210987, Payload: []byte("hello world")}

	got, err := cache.Decode(want.Encode())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.RowVersion != want.RowVersion || got.FillVersion != want.FillVersion {
		t.Errorf("versions = %d/%d, want %d/%d",
			got.RowVersion, got.FillVersion, want.RowVersion, want.FillVersion)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("payload = %q, want %q", got.Payload, want.Payload)
	}
}

func TestEmptyPayloadRoundTrips(t *testing.T) {
	t.Parallel()

	got, err := cache.Decode(cache.Entry{RowVersion: 1, FillVersion: 2}.Encode())
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	// A row whose payload is legitimately empty must not be confused with a missing entry.
	if got.Payload == nil || len(got.Payload) != 0 {
		t.Errorf("payload = %v, want an empty non-nil slice", got.Payload)
	}
}

func TestTruncatedEntryIsRejected(t *testing.T) {
	t.Parallel()

	full := cache.Entry{RowVersion: 1, FillVersion: 2, Payload: []byte("abc")}.Encode()

	// A partially written or corrupted entry must be an error, never a silently short payload:
	// serving truncated data would be indistinguishable from serving a stale row, and this project
	// exists to tell those apart.
	for n := 0; n < len(full)-3; n++ {
		if _, err := cache.Decode(full[:n]); err == nil {
			t.Errorf("Decode accepted a %d-byte entry (full is %d)", n, len(full))
		}
	}
}

func TestUnknownEncodingVersionIsRejected(t *testing.T) {
	t.Parallel()

	b := cache.Entry{RowVersion: 1, FillVersion: 2, Payload: []byte("x")}.Encode()
	b[0] = 0xFF

	// Entries outlive deploys. An old process meeting a new encoding must refuse it rather than
	// misread the bytes as versions, which would corrupt the compare-and-set the whole model rests
	// on (CONSISTENCY.md §2).
	if _, err := cache.Decode(b); err == nil {
		t.Error("Decode accepted an entry with an unknown encoding version")
	}
}

func TestEncodingIsStable(t *testing.T) {
	t.Parallel()

	e := cache.Entry{RowVersion: 0x0102030405060708, FillVersion: 0x1112131415161718, Payload: []byte("ab")}
	a, b := e.Encode(), e.Encode()

	if !bytes.Equal(a, b) {
		t.Error("Encode is not deterministic")
	}
	// version byte + two uint64s + payload
	if want := 1 + 8 + 8 + 2; len(a) != want {
		t.Errorf("encoded length = %d, want %d", len(a), want)
	}
}
