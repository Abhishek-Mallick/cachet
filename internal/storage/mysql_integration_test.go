//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

func TestPutThenGetReturnsTheRow(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	want := storage.Record{ID: 1, TenantID: 7, Status: 2, Payload: []byte("hello")}
	ver, err := sh.Put(ctx, want)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, fill, err := sh.Get(ctx, want.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != want.ID || got.TenantID != want.TenantID || got.Status != want.Status {
		t.Errorf("Get returned %+v, want id/tenant/status %d/%d/%d", got, want.ID, want.TenantID, want.Status)
	}
	if !bytes.Equal(got.Payload, want.Payload) {
		t.Errorf("payload = %q, want %q", got.Payload, want.Payload)
	}
	if got.Version != ver {
		t.Errorf("row version = %v, want the version Put returned (%v)", got.Version, ver)
	}
	// The fill version says "this read reflects shard state as of fill". It must be at least the
	// version of the row it returned, or a session that just wrote this row would reject its own
	// read (CONSISTENCY.md §1).
	if fill < ver {
		t.Errorf("fill version %v precedes the row version %v it returned", fill, ver)
	}
}

func TestGetMissingRowReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	// "This row does not exist" is a cacheable fact (negative caching, product spec §6 Tier 0), so
	// absence has to be a distinguishable, typed outcome rather than a zero value.
	if _, _, err := sh.Get(ctx, 999_999); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get on a missing row returned %v, want ErrNotFound", err)
	}
}

func TestPutStampsStrictlyIncreasingVersions(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	rec := storage.Record{ID: 2, TenantID: 1, Status: 0, Payload: []byte("v1")}
	var last storage.Version
	for i := 0; i < 20; i++ {
		rec.Payload = []byte(time.Now().String())
		v, err := sh.Put(ctx, rec)
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if v <= last {
			t.Fatalf("Put %d returned version %v, not greater than the previous %v", i, v, last)
		}
		last = v
	}
}

func TestPutAdoptsAVersionWrittenByAnotherEngine(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	// Simulate a second engine instance whose clock runs far ahead having written this row.
	future := storage.NewVersion(time.Now().Add(2*time.Hour).UnixMilli(), 0)
	forceRowVersion(ctx, t, sh, storage.Record{ID: 3, TenantID: 1, Payload: []byte("theirs")}, future)

	// Our own write must exceed theirs even though our wall clock is two hours behind it. Without
	// this, per-shard version monotonicity breaks the moment a second engine joins, and a stale
	// fill wins a compare-and-set. ADR 0003.
	got, err := sh.Put(ctx, storage.Record{ID: 3, TenantID: 1, Payload: []byte("ours")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got <= future {
		t.Errorf("Put stamped %v, which does not exceed the pre-existing row version %v", got, future)
	}
}

func TestDeleteRemovesTheRowAndReturnsAVersion(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	rec := storage.Record{ID: 4, TenantID: 1, Payload: []byte("doomed")}
	written, err := sh.Put(ctx, rec)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	deleted, err := sh.Delete(ctx, rec.ID)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// The delete's version is what the invalidation is stamped with, so it must outrank the write
	// it supersedes or the tombstone loses its own compare-and-set.
	if deleted <= written {
		t.Errorf("Delete returned %v, which does not exceed the write it removed (%v)", deleted, written)
	}
	if _, _, err := sh.Get(ctx, rec.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Get after Delete returned %v, want ErrNotFound", err)
	}
}

func TestDeleteOfAMissingRowReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	if _, err := sh.Delete(ctx, 888_888); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Delete on a missing row returned %v, want ErrNotFound", err)
	}
}

func TestBatchGetReturnsOnlyExistingRowsInOneRoundTrip(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	for _, id := range []uint64{10, 11, 12} {
		if _, err := sh.Put(ctx, storage.Record{ID: id, TenantID: 1, Payload: []byte("x")}); err != nil {
			t.Fatalf("Put %d: %v", id, err)
		}
	}

	got, fill, err := sh.BatchGet(ctx, []uint64{10, 11, 12, 13})
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("BatchGet returned %d rows, want 3 (id 13 does not exist)", len(got))
	}
	// Absent ids are omitted rather than represented by zero values: the caller distinguishes
	// "missing" from "present but empty" by presence in the map, which is what negative caching
	// needs downstream.
	if _, present := got[13]; present {
		t.Error("BatchGet returned an entry for a row that does not exist")
	}
	for id, rec := range got {
		if fill < rec.Version {
			t.Errorf("fill version %v precedes row %d's version %v", fill, id, rec.Version)
		}
	}
}

func TestBatchGetOfNoKeysDoesNotQuery(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	got, _, err := sh.BatchGet(ctx, nil)
	if err != nil {
		t.Fatalf("BatchGet(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("BatchGet(nil) returned %d rows, want 0", len(got))
	}
}

func TestGetRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sh := openTestShard(ctx, t)
	cancel()

	// Every read carries a deadline; abandoning a slow shard is the tail-latency mechanism this
	// whole system is built around (ADR 0001). A query that ignores its context cannot be abandoned.
	if _, _, err := sh.Get(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("Get with a cancelled context returned %v, want context.Canceled", err)
	}
}
