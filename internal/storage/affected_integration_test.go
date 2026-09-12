//go:build integration

package storage_test

import (
	"context"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// A conditional write does not name the rows it touches. CONSISTENCY.md §5: the engine resolves
// them exactly, inside the transaction, with SELECT ... FOR UPDATE — and when the predicate matches
// more rows than the budget allows, it gives up on exact resolution and says so rather than
// pretending.
//
// Resolving INSIDE the transaction is the whole design. Resolving before it would leave a window in
// which a row joins or leaves the predicate between the SELECT and the UPDATE: the write would touch
// a row nobody invalidated, and the resulting staleness would look like a cache bug for as long as
// anyone cared to investigate it.

func seedTenant(ctx context.Context, t *testing.T, sh *storage.Shard, tenant uint32, ids []uint64, status uint8) {
	t.Helper()

	for _, id := range ids {
		if _, err := sh.Put(ctx, storage.Record{
			ID: id, TenantID: tenant, Status: status, Payload: []byte("seed"),
		}); err != nil {
			t.Fatalf("seed %d: %v", id, err)
		}
	}
}

func TestUpdateWhereReturnsExactlyTheRowsItTouched(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	const tenant = 7700
	ids := []uint64{7_700_001, 7_700_002, 7_700_003}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	// A row in the same tenant that the predicate must NOT match, and a row in another tenant.
	seedTenant(ctx, t, sh, tenant, []uint64{7_700_004}, 9)
	seedTenant(ctx, t, sh, 7701, []uint64{7_700_005}, 1)

	res, err := sh.UpdateWhere(ctx, storage.Predicate{
		TenantID: tenant, MatchStatus: 1, SetStatus: 2,
	}, 1000)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}

	if res.Degraded {
		t.Errorf("a 3-row predicate degraded under a 1000-row budget: %+v", res)
	}
	if len(res.AffectedIDs) != len(ids) {
		t.Fatalf("AffectedIDs = %v, want exactly %v", res.AffectedIDs, ids)
	}

	got := make(map[uint64]bool, len(res.AffectedIDs))
	for _, id := range res.AffectedIDs {
		got[id] = true
	}
	for _, want := range ids {
		if !got[want] {
			t.Errorf("row %d was updated but is missing from AffectedIDs; its cache entry would "+
				"never be invalidated", want)
		}
	}
	if got[7_700_004] || got[7_700_005] {
		t.Error("AffectedIDs names a row the predicate did not match; invalidating it would cost " +
			"hit rate for no reason")
	}
}

func TestUpdateWhereStampsEveryRowWithTheCommitVersion(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	const tenant = 7710
	ids := []uint64{7_710_001, 7_710_002}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	res, err := sh.UpdateWhere(ctx, storage.Predicate{TenantID: tenant, MatchStatus: 1, SetStatus: 3}, 1000)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}
	if res.Version == 0 {
		t.Fatal("UpdateWhere returned no version; there would be nothing to stamp the invalidation with")
	}

	// Every touched row must carry the commit version. The cache tombstone is stamped with it, so a
	// row left at an older version would let a racing fill win the compare-and-set and resurrect
	// the value the write just replaced.
	for _, id := range ids {
		rec, _, err := sh.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get %d: %v", id, err)
		}
		if rec.Version != res.Version {
			t.Errorf("row %d is at version %d, but the write committed at %d", id, rec.Version, res.Version)
		}
		if rec.Status != 3 {
			t.Errorf("row %d has status %d, want 3; the predicate matched it but did not change it", id, rec.Status)
		}
	}
}

func TestUpdateWhereVersionOutranksTheRowsItReplaced(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	const tenant = 7720
	ids := []uint64{7_720_001}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	before, _, err := sh.Get(ctx, ids[0])
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	res, err := sh.UpdateWhere(ctx, storage.Predicate{TenantID: tenant, MatchStatus: 1, SetStatus: 2}, 1000)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}

	// The invalidation is stamped with this version, so it has to outrank what it supersedes or the
	// tombstone loses its own compare-and-set.
	if res.Version <= before.Version {
		t.Errorf("the conditional write committed at %d, no higher than the row it replaced (%d)",
			res.Version, before.Version)
	}
}

func TestUpdateWhereDegradesPastTheBudgetInsteadOfResolvingEveryKey(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	const tenant = 7730
	var ids []uint64
	for i := uint64(0); i < 12; i++ {
		ids = append(ids, 7_730_001+i)
	}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	// A budget far below the match count. Resolving a million keys exactly would hold a transaction
	// open across a million row locks, which is a worse outage than the staleness it prevents.
	res, err := sh.UpdateWhere(ctx, storage.Predicate{TenantID: tenant, MatchStatus: 1, SetStatus: 2}, 5)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}

	if !res.Degraded {
		t.Error("a 12-row predicate under a 5-row budget did not report degraded")
	}
	if len(res.AffectedIDs) != 0 {
		t.Errorf("a degraded write returned %d affected ids; a PARTIAL key list is worse than none, "+
			"because the caller cannot tell which rows were left to CDC", len(res.AffectedIDs))
	}
	if res.Version == 0 {
		t.Error("a degraded write returned no version; the writer's own session guarantee depends on it")
	}
}

func TestADegradedWriteStillApplies(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	const tenant = 7740
	var ids []uint64
	for i := uint64(0); i < 10; i++ {
		ids = append(ids, 7_740_001+i)
	}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	res, err := sh.UpdateWhere(ctx, storage.Predicate{TenantID: tenant, MatchStatus: 1, SetStatus: 4}, 3)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}
	if !res.Degraded {
		t.Fatal("precondition: expected this write to degrade")
	}

	// Degraded describes the INVALIDATION, never the write. The write is a committed database
	// change either way; what was given up is the exact key list, not the durability.
	for _, id := range ids {
		rec, _, err := sh.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get %d: %v", id, err)
		}
		if rec.Status != 4 {
			t.Errorf("row %d has status %d after a degraded write, want 4; the write did not apply", id, rec.Status)
		}
		if rec.Version != res.Version {
			t.Errorf("row %d is at version %d, want the commit version %d", id, rec.Version, res.Version)
		}
	}
}

func TestUpdateWhereMatchingNothingIsNotAnError(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	res, err := sh.UpdateWhere(ctx, storage.Predicate{TenantID: 7750, MatchStatus: 1, SetStatus: 2}, 1000)
	if err != nil {
		t.Fatalf("UpdateWhere matching no rows: %v", err)
	}
	if res.Degraded {
		t.Error("a predicate matching nothing reported degraded")
	}
	if len(res.AffectedIDs) != 0 {
		t.Errorf("AffectedIDs = %v, want none", res.AffectedIDs)
	}
	if res.Matched != 0 {
		t.Errorf("Matched = %d, want 0", res.Matched)
	}
}

func TestUpdateWhereRejectsANonPositiveBudget(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	// A zero budget would degrade every write, silently turning exact invalidation off across the
	// whole system. That has to be a configuration error, not a quiet mode change.
	if _, err := sh.UpdateWhere(ctx, storage.Predicate{TenantID: 1, MatchStatus: 1, SetStatus: 2}, 0); err == nil {
		t.Error("UpdateWhere accepted a zero key budget")
	}
}

func TestUpdateWhereReportsHowManyRowsItMatched(t *testing.T) {
	ctx := context.Background()
	sh := openTestShard(ctx, t)

	const tenant = 7760
	ids := []uint64{7_760_001, 7_760_002, 7_760_003, 7_760_004}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	res, err := sh.UpdateWhere(ctx, storage.Predicate{TenantID: tenant, MatchStatus: 1, SetStatus: 2}, 1000)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}
	// The blast radius, reported so `cachetctl invalidate --dry-run` and the SDK can show it before
	// anyone commits to it.
	if res.Matched != len(ids) {
		t.Errorf("Matched = %d, want %d", res.Matched, len(ids))
	}
}
