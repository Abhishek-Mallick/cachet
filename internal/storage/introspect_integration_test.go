//go:build integration

package storage_test

import (
	"context"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// Reading the real schema is the step that turns a drifted declaration from a 3am column-not-found
// into a refusal to start. It is worth testing against a real MySQL rather than a fake, because the
// INFORMATION_SCHEMA details — DATABASE(), IS_NULLABLE as a string, SEQ_IN_INDEX ordering — are
// exactly what a fake would get wrong in the same direction as the code.
func TestIntrospectReadsTheFixtureTable(t *testing.T) {
	ctx := context.Background()
	db := storage.DBForTest(openTestShard(ctx, t))

	cols, indexes, err := storage.Introspect(ctx, db, "entities")
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}

	want := []string{"id", "tenant_id", "status", "payload", "version", "updated_at"}
	if len(cols) != len(want) {
		t.Fatalf("got %d columns, want %d: %+v", len(cols), len(want), cols)
	}
	for i, name := range want {
		if cols[i].Name != name {
			t.Errorf("column %d is %q, want %q — ORDINAL_POSITION ordering is not being honoured", i, cols[i].Name, name)
		}
		if cols[i].Nullable {
			t.Errorf("%s reports nullable; the fixture declares every column NOT NULL", name)
		}
	}

	// The declaration must verify against what the table really is.
	if err := storage.VerifyAgainstLive(entitiesDescriptor(t), cols); err != nil {
		t.Errorf("the shipped descriptor does not match the shipped schema: %v", err)
	}

	// And the predicate the conditional write uses must be covered, in the right order.
	var tenantStatus *storage.Index
	for i := range indexes {
		if indexes[i].Name == "idx_tenant_status" {
			tenantStatus = &indexes[i]
		}
	}
	if tenantStatus == nil {
		t.Fatalf("idx_tenant_status not reported: %+v", indexes)
	}
	if len(tenantStatus.Columns) != 2 || tenantStatus.Columns[0] != "tenant_id" || tenantStatus.Columns[1] != "status" {
		t.Errorf("index columns = %v, want [tenant_id status] in key order", tenantStatus.Columns)
	}

	if _, err := storage.NewTable(entitiesDescriptor(t), indexes,
		storage.PredicateSpec{Match: []string{"tenant_id", "status"}, Set: []string{"status"}}); err != nil {
		t.Errorf("the conditional write Cachet ships with was refused: %v", err)
	}
}

func TestIntrospectRefusesAMissingTable(t *testing.T) {
	ctx := context.Background()
	db := storage.DBForTest(openTestShard(ctx, t))

	if _, _, err := storage.Introspect(ctx, db, "no_such_table"); err == nil {
		t.Error("a table that does not exist was accepted")
	}
}
