//go:build integration

package storage_test

import (
	"context"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
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

// matchTenantStatus and setStatus spell the one declared predicate this fixture table has.
// rowVersionOf and rowStatusOf read the fixture table's columns by declared position, which is how
// every reader of a generic row gets at one.
func rowVersionOf(t *testing.T, row storage.Row) storage.Version {
	t.Helper()

	v, err := row[4].Uint64()
	if err != nil {
		t.Fatalf("version column: %v", err)
	}
	return storage.Version(v)
}

func rowStatusOf(t *testing.T, row storage.Row) uint64 {
	t.Helper()

	v, err := row[2].Uint64()
	if err != nil {
		t.Fatalf("status column: %v", err)
	}
	return v
}

func matchTenantStatus(tenant uint32, status uint8) []storage.ColumnValue {
	return []storage.ColumnValue{
		{Column: "tenant_id", Value: schema.Uint(uint64(tenant))},
		{Column: "status", Value: schema.Uint(uint64(status))},
	}
}

func setStatus(status uint8) []storage.ColumnValue {
	return []storage.ColumnValue{{Column: "status", Value: schema.Uint(uint64(status))}}
}

func seedTenant(ctx context.Context, t *testing.T, sh *storage.Shard, tenant uint32, ids []uint64, status uint8) {
	t.Helper()

	for _, id := range ids {
		if _, err := sh.PutRow(ctx, entitiesTable, entityRow(id, tenant, status, "seed")); err != nil {
			t.Fatalf("seed %d: %v", id, err)
		}
	}
}

func TestUpdateWhereReturnsExactlyTheRowsItTouched(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	const tenant = 7700
	ids := []uint64{7_700_001, 7_700_002, 7_700_003}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	// A row in the same tenant that the predicate must NOT match, and a row in another tenant.
	seedTenant(ctx, t, sh, tenant, []uint64{7_700_004}, 9)
	seedTenant(ctx, t, sh, 7701, []uint64{7_700_005}, 1)

	res, err := sh.UpdateWhere(ctx, entitiesTable, matchTenantStatus(tenant, 1), setStatus(2), 1000)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}

	if res.Degraded {
		t.Errorf("a 3-row predicate degraded under a 1000-row budget: %+v", res)
	}
	if len(res.AffectedKeys) != len(ids) {
		t.Fatalf("AffectedKeys = %v, want exactly %v", res.AffectedKeys, ids)
	}

	got := make(map[string]bool, len(res.AffectedKeys))
	for _, k := range res.AffectedKeys {
		got[k.String()] = true
	}
	for _, want := range ids {
		if !got[entityKey(t, want).String()] {
			t.Errorf("row %d was updated but is missing from AffectedKeys; its cache entry would "+
				"never be invalidated", want)
		}
	}
	if got[entityKey(t, 7_700_004).String()] || got[entityKey(t, 7_700_005).String()] {
		t.Error("AffectedKeys names a row the predicate did not match; invalidating it would cost " +
			"hit rate for no reason")
	}
}

func TestUpdateWhereStampsEveryRowWithTheCommitVersion(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	const tenant = 7710
	ids := []uint64{7_710_001, 7_710_002}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	res, err := sh.UpdateWhere(ctx, entitiesTable, matchTenantStatus(tenant, 1), setStatus(3), 1000)
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
		row, _, err := sh.GetRow(ctx, entityKey(t, id))
		if err != nil {
			t.Fatalf("Get %d: %v", id, err)
		}
		if rowVersionOf(t, row) != res.Version {
			t.Errorf("row %d is at version %d, but the write committed at %d", id, rowVersionOf(t, row), res.Version)
		}
		if rowStatusOf(t, row) != 3 {
			t.Errorf("row %d has status %d, want 3; the predicate matched it but did not change it", id, rowStatusOf(t, row))
		}
	}
}

func TestUpdateWhereVersionOutranksTheRowsItReplaced(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	const tenant = 7720
	ids := []uint64{7_720_001}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	before, _, err := sh.GetRow(ctx, entityKey(t, ids[0]))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	res, err := sh.UpdateWhere(ctx, entitiesTable, matchTenantStatus(tenant, 1), setStatus(2), 1000)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}

	// The invalidation is stamped with this version, so it has to outrank what it supersedes or the
	// tombstone loses its own compare-and-set.
	if res.Version <= rowVersionOf(t, before) {
		t.Errorf("the conditional write committed at %d, no higher than the row it replaced (%d)",
			res.Version, rowVersionOf(t, before))
	}
}

func TestUpdateWhereDegradesPastTheBudgetInsteadOfResolvingEveryKey(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	const tenant = 7730
	var ids []uint64
	for i := uint64(0); i < 12; i++ {
		ids = append(ids, 7_730_001+i)
	}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	// A budget far below the match count. Resolving a million keys exactly would hold a transaction
	// open across a million row locks, which is a worse outage than the staleness it prevents.
	res, err := sh.UpdateWhere(ctx, entitiesTable, matchTenantStatus(tenant, 1), setStatus(2), 5)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}

	if !res.Degraded {
		t.Error("a 12-row predicate under a 5-row budget did not report degraded")
	}
	if len(res.AffectedKeys) != 0 {
		t.Errorf("a degraded write returned %d affected ids; a PARTIAL key list is worse than none, "+
			"because the caller cannot tell which rows were left to CDC", len(res.AffectedKeys))
	}
	if res.Version == 0 {
		t.Error("a degraded write returned no version; the writer's own session guarantee depends on it")
	}
}

func TestADegradedWriteStillApplies(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	const tenant = 7740
	var ids []uint64
	for i := uint64(0); i < 10; i++ {
		ids = append(ids, 7_740_001+i)
	}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	res, err := sh.UpdateWhere(ctx, entitiesTable, matchTenantStatus(tenant, 1), setStatus(4), 3)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}
	if !res.Degraded {
		t.Fatal("precondition: expected this write to degrade")
	}

	// Degraded describes the INVALIDATION, never the write. The write is a committed database
	// change either way; what was given up is the exact key list, not the durability.
	for _, id := range ids {
		row, _, err := sh.GetRow(ctx, entityKey(t, id))
		if err != nil {
			t.Fatalf("Get %d: %v", id, err)
		}
		if rowStatusOf(t, row) != 4 {
			t.Errorf("row %d has status %d after a degraded write, want 4; the write did not apply", id, rowStatusOf(t, row))
		}
		if rowVersionOf(t, row) != res.Version {
			t.Errorf("row %d is at version %d, want the commit version %d", id, rowVersionOf(t, row), res.Version)
		}
	}
}

func TestUpdateWhereMatchingNothingIsNotAnError(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	res, err := sh.UpdateWhere(ctx, entitiesTable, matchTenantStatus(7750, 1), setStatus(2), 1000)
	if err != nil {
		t.Fatalf("UpdateWhere matching no rows: %v", err)
	}
	if res.Degraded {
		t.Error("a predicate matching nothing reported degraded")
	}
	if len(res.AffectedKeys) != 0 {
		t.Errorf("AffectedKeys = %v, want none", res.AffectedKeys)
	}
	if res.Matched != 0 {
		t.Errorf("Matched = %d, want 0", res.Matched)
	}
}

func TestUpdateWhereRejectsANonPositiveBudget(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	// A zero budget would degrade every write, silently turning exact invalidation off across the
	// whole system. That has to be a configuration error, not a quiet mode change.
	if _, err := sh.UpdateWhere(ctx, entitiesTable, matchTenantStatus(1, 1), setStatus(2), 0); err == nil {
		t.Error("UpdateWhere accepted a zero key budget")
	}
}

func TestUpdateWhereReportsHowManyRowsItMatched(t *testing.T) {
	ctx := context.Background()
	sh := entitiesShard(ctx, t)

	const tenant = 7760
	ids := []uint64{7_760_001, 7_760_002, 7_760_003, 7_760_004}
	seedTenant(ctx, t, sh, tenant, ids, 1)

	res, err := sh.UpdateWhere(ctx, entitiesTable, matchTenantStatus(tenant, 1), setStatus(2), 1000)
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}
	// The blast radius, reported so `cachetctl invalidate --dry-run` and the SDK can show it before
	// anyone commits to it.
	if res.Matched != len(ids) {
		t.Errorf("Matched = %d, want %d", res.Matched, len(ids))
	}
}
