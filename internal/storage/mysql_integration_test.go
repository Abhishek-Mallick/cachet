//go:build integration

package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

func TestPutThenGetReturnsTheRow(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	ver, err := sh.PutRow(ctx, entitiesTable, entityRow(1, 7, 2, "hello"))
	if err != nil {
		t.Fatalf("PutRow: %v", err)
	}

	got, fill, err := sh.GetRow(ctx, entityKey(t, 1))
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}
	for i, want := range []string{"1", "7", "2", "hello"} {
		if string(got[i].Bytes) != want {
			t.Errorf("column %d = %q, want %q", i, got[i].Bytes, want)
		}
	}
	if v := rowVersionOf(t, got); v != ver {
		t.Errorf("row version = %v, want the version PutRow returned (%v)", v, ver)
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
	sh := entitiesShard(ctx, t)

	// "This row does not exist" is a cacheable fact (negative caching, product spec §6 Tier 0), so
	// absence has to be a distinguishable, typed outcome rather than a zero value.
	if _, _, err := sh.GetRow(ctx, entityKey(t, 999_999)); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("GetRow on a missing row returned %v, want ErrNotFound", err)
	}
}

func TestPutStampsStrictlyIncreasingVersions(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	var last storage.Version
	for i := 0; i < 20; i++ {
		v, err := sh.PutRow(ctx, entitiesTable, entityRow(2, 1, 0, time.Now().String()))
		if err != nil {
			t.Fatalf("PutRow %d: %v", i, err)
		}
		if v <= last {
			t.Fatalf("PutRow %d returned version %v, not greater than the previous %v", i, v, last)
		}
		last = v
	}
}

func TestPutAdoptsAVersionWrittenByAnotherEngine(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	// Simulate a second engine instance whose clock runs far ahead having written this row.
	future := storage.NewVersion(time.Now().Add(2*time.Hour).UnixMilli(), 0)
	forceRowVersion(ctx, t, sh, entityRow(3, 1, 0, "theirs"), future)

	// Our own write must exceed theirs even though our wall clock is two hours behind it. Without
	// this, per-shard version monotonicity breaks the moment a second engine joins, and a stale
	// fill wins a compare-and-set. ADR 0003.
	got, err := sh.PutRow(ctx, entitiesTable, entityRow(3, 1, 0, "ours"))
	if err != nil {
		t.Fatalf("PutRow: %v", err)
	}
	if got <= future {
		t.Errorf("PutRow stamped %v, which does not exceed the pre-existing row version %v", got, future)
	}
}

// TestDeleteAdoptsAVersionWrittenByAnotherEngine is the delete half of the same property, and it
// had no test until the generic write path was written and the gap showed.
//
// A delete's version is what its tombstone carries. If it does not outrank the row it removes, the
// compare-and-set fails and the deleted row keeps being served from the cache until its TTL.
func TestDeleteAdoptsAVersionWrittenByAnotherEngine(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	future := storage.NewVersion(time.Now().Add(2*time.Hour).UnixMilli(), 0)
	forceRowVersion(ctx, t, sh, entityRow(5, 1, 0, "theirs"), future)

	got, err := sh.DeleteRow(ctx, entityKey(t, 5))
	if err != nil {
		t.Fatalf("DeleteRow: %v", err)
	}
	if got <= future {
		t.Errorf("DeleteRow stamped %v, which does not exceed the row version it removed (%v); "+
			"the tombstone would lose its compare-and-set and the row would stay cached", got, future)
	}
}

func TestDeleteRemovesTheRowAndReturnsAVersion(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	written, err := sh.PutRow(ctx, entitiesTable, entityRow(4, 1, 0, "doomed"))
	if err != nil {
		t.Fatalf("PutRow: %v", err)
	}

	deleted, err := sh.DeleteRow(ctx, entityKey(t, 4))
	if err != nil {
		t.Fatalf("DeleteRow: %v", err)
	}
	// The delete's version is what the invalidation is stamped with, so it must outrank the write
	// it supersedes or the tombstone loses its own compare-and-set.
	if deleted <= written {
		t.Errorf("DeleteRow returned %v, which does not exceed the write it removed (%v)", deleted, written)
	}
	if _, _, err := sh.GetRow(ctx, entityKey(t, 4)); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("GetRow after DeleteRow returned %v, want ErrNotFound", err)
	}
}

func TestDeleteOfAMissingRowReturnsErrNotFound(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	if _, err := sh.DeleteRow(ctx, entityKey(t, 888_888)); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("DeleteRow on a missing row returned %v, want ErrNotFound", err)
	}
}

func TestBatchGetReturnsOnlyExistingRowsInOneRoundTrip(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	for _, id := range []uint64{10, 11, 12} {
		if _, err := sh.PutRow(ctx, entitiesTable, entityRow(id, 1, 0, "x")); err != nil {
			t.Fatalf("PutRow %d: %v", id, err)
		}
	}

	keys := []schema.Key{entityKey(t, 10), entityKey(t, 11), entityKey(t, 12), entityKey(t, 13)}
	got, fill, err := sh.BatchGetRows(ctx, keys)
	if err != nil {
		t.Fatalf("BatchGetRows: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("BatchGetRows returned %d rows, want 3 (id 13 does not exist)", len(got))
	}
	// Absent ids are omitted rather than represented by zero values: the caller distinguishes
	// "missing" from "present but empty" by presence in the map, which is what negative caching
	// needs downstream.
	if _, present := got[entityKey(t, 13).String()]; present {
		t.Error("BatchGetRows returned an entry for a row that does not exist")
	}
	for key, row := range got {
		if v := rowVersionOf(t, row); fill < v {
			t.Errorf("fill version %v precedes row %s's version %v", fill, key, v)
		}
	}
}

func TestBatchGetOfNoKeysDoesNotQuery(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	got, _, err := sh.BatchGetRows(ctx, nil)
	if err != nil {
		t.Fatalf("BatchGetRows(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("BatchGetRows(nil) returned %d rows, want 0", len(got))
	}
}

func TestGetRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sh := entitiesShard(ctx, t)
	cancel()

	// Every read carries a deadline; abandoning a slow shard is the tail-latency mechanism this
	// whole system is built around (ADR 0001). A query that ignores its context cannot be abandoned.
	if _, _, err := sh.GetRow(ctx, entityKey(t, 1)); !errors.Is(err, context.Canceled) {
		t.Errorf("GetRow with a cancelled context returned %v, want context.Canceled", err)
	}
}
