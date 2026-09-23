package storage_test

import (
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

func entitiesDescriptor(t *testing.T) *schema.Descriptor {
	t.Helper()

	d, err := schema.NewDescriptor(schema.TableConfig{
		Name: "entities", PrimaryKey: []string{"id"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: schema.Uint64},
			{Name: "tenant_id", Type: schema.Uint32},
			{Name: "status", Type: schema.Uint8},
			{Name: "payload", Type: schema.Bytes},
			{Name: "version", Type: schema.Uint64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// Statements are built once, at boot, from identifiers that were validated before they got here.
// Nothing on the request path concatenates SQL.
func TestStatementsAreBuiltFromTheDescriptor(t *testing.T) {
	t.Parallel()

	tbl, err := storage.NewTable(entitiesDescriptor(t), nil)
	if err != nil {
		t.Fatal(err)
	}

	for name, got := range map[string]string{
		"get":    tbl.GetStmt(),
		"insert": tbl.InsertStmt(),
		"update": tbl.UpdateStmt(),
		"delete": tbl.DeleteStmt(),
	} {
		if !strings.Contains(got, "`entities`") {
			t.Errorf("%s does not name the table with a quoted identifier: %s", name, got)
		}
		if strings.Contains(got, ";") {
			t.Errorf("%s contains a statement separator: %s", name, got)
		}
	}

	// The select list is the declared column order, which is what the row codec assumes.
	get := tbl.GetStmt()
	for _, col := range []string{"`id`", "`tenant_id`", "`status`", "`payload`", "`version`"} {
		if !strings.Contains(get, col) {
			t.Errorf("get statement is missing %s: %s", col, get)
		}
	}
	if strings.Index(get, "`id`") > strings.Index(get, "`tenant_id`") {
		t.Errorf("select list is not in declared column order: %s", get)
	}
}

// Hazard 2. A predicate on an unindexed column makes `SELECT … FOR UPDATE` take gap locks across
// the table INSIDE a transaction that is already holding the write. That is an outage, not a slow
// query, and it appears only under the concurrency that production has and a test does not.
func TestAPredicateWithoutIndexCoverageIsRefused(t *testing.T) {
	t.Parallel()

	d := entitiesDescriptor(t)
	indexes := []storage.Index{
		{Name: "PRIMARY", Columns: []string{"id"}},
		{Name: "idx_tenant_status", Columns: []string{"tenant_id", "status"}},
		{Name: "idx_version", Columns: []string{"version"}},
	}

	// Declared, and covered by a prefix of idx_tenant_status.
	if _, err := storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"tenant_id", "status"}}); err != nil {
		t.Errorf("refused a predicate the index covers: %v", err)
	}
	if _, err := storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"tenant_id"}}); err != nil {
		t.Errorf("refused a predicate covered by the index's leading column: %v", err)
	}

	// Not covered: payload has no index at all.
	_, err := storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"payload"}})
	if err == nil {
		t.Fatal("accepted a predicate on an unindexed column: SELECT ... FOR UPDATE would gap-lock the table")
	}
	if !strings.Contains(err.Error(), "index") {
		t.Errorf("the error does not name the problem: %v", err)
	}

	// Covered only as a NON-leading column, which MySQL cannot use for this predicate.
	if _, err := storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"status"}}); err == nil {
		t.Error("accepted a predicate on a non-leading index column")
	}
}

func TestAPredicateNamingAnUndeclaredColumnIsRefused(t *testing.T) {
	t.Parallel()

	d := entitiesDescriptor(t)
	indexes := []storage.Index{{Name: "PRIMARY", Columns: []string{"id"}}}

	if _, err := storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"nope"}}); err == nil {
		t.Error("accepted a predicate on a column that is not declared")
	}
	if _, err := storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"id"}, Set: []string{"nope"}}); err == nil {
		t.Error("accepted a SET on a column that is not declared")
	}
	// The version column is maintained by Cachet; letting a predicate set it would let an
	// application write a version the engine did not issue.
	if _, err := storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"id"}, Set: []string{"version"}}); err == nil {
		t.Error("accepted a SET on the version column")
	}
}

// A declaration that does not match the database is a configuration error, and it must be found at
// boot rather than by a query that fails later with a column-not-found from MySQL.
func TestTheDeclarationIsCheckedAgainstTheLiveSchema(t *testing.T) {
	t.Parallel()

	d := entitiesDescriptor(t)

	live := []storage.LiveColumn{
		{Name: "id", Nullable: false},
		{Name: "tenant_id", Nullable: false},
		{Name: "status", Nullable: false},
		{Name: "payload", Nullable: false},
		{Name: "version", Nullable: false},
		{Name: "updated_at", Nullable: false},
	}
	if err := storage.VerifyAgainstLive(d, live); err != nil {
		t.Errorf("a correct declaration was refused: %v", err)
	}

	// A declared column the table does not have.
	missing := live[:4]
	if err := storage.VerifyAgainstLive(d, missing); err == nil {
		t.Error("accepted a declaration naming a column the table does not have")
	}

	// Declared NOT NULL, actually nullable: the codec would refuse the first NULL it met, at
	// runtime, on whichever row happened to have one.
	wrong := append([]storage.LiveColumn(nil), live...)
	wrong[3].Nullable = true
	if err := storage.VerifyAgainstLive(d, wrong); err == nil {
		t.Error("accepted a column declared NOT NULL that is nullable in the database")
	}
}
