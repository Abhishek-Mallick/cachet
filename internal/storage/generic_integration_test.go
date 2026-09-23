//go:build integration

package storage_test

import (
	"context"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// The point of the whole ship, stated as a test: a table that is not the fixture.
//
// Different name, a STRING primary key, nullable columns, and a column Cachet does not cache. If
// this passes, Cachet is no longer a system that caches `entities`.
const ordersDDL = `
CREATE TABLE IF NOT EXISTS orders (
  order_ref   VARCHAR(64)      NOT NULL,
  customer    VARCHAR(128)     NOT NULL,
  note        TEXT             NULL,
  total       DECIMAL(10,2)    NOT NULL,
  version     BIGINT UNSIGNED  NOT NULL,
  created_at  TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (order_ref),
  KEY idx_customer (customer)
) ENGINE=ROCKSDB DEFAULT COLLATE=utf8mb4_bin`

func ordersDescriptor(t *testing.T) *schema.Descriptor {
	t.Helper()

	d, err := schema.NewDescriptor(schema.TableConfig{
		Name: "orders", PrimaryKey: []string{"order_ref"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "order_ref", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "customer", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "note", Type: schema.Text, Nullable: true},
			{Name: "total", Type: schema.Text},
			{Name: "version", Type: schema.Uint64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func ordersShard(ctx context.Context, t *testing.T) *storage.Shard {
	t.Helper()

	sh := openTestShard(ctx, t)
	db := storage.DBForTest(sh)
	if _, err := db.ExecContext(ctx, ordersDDL); err != nil {
		t.Fatalf("create orders: %v", err)
	}

	d := ordersDescriptor(t)
	cols, indexes, err := storage.Introspect(ctx, db, "orders")
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	// The declaration is checked against the real table, exactly as boot does it.
	if err := storage.VerifyAgainstLive(d, cols); err != nil {
		t.Fatalf("the declaration does not match the table: %v", err)
	}
	tbl, err := storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"customer"}, Set: []string{"note"}})
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	return sh.WithTable(tbl)
}

func TestAnArbitraryTableRoundTrips(t *testing.T) {
	ctx := context.Background()
	sh := ordersShard(ctx, t)
	d := sh.Table().Descriptor()

	key, err := d.Key("ORD-001")
	if err != nil {
		t.Fatal(err)
	}

	row := storage.Row{
		schema.Str("ORD-001"),
		schema.Str("alice"),
		schema.Null(), // note is NULL
		schema.Str("19.99"),
		schema.Uint(0), // replaced by the shard
	}
	v, err := sh.PutRow(ctx, row)
	if err != nil {
		t.Fatalf("PutRow: %v", err)
	}
	if v == 0 {
		t.Fatal("PutRow returned version 0")
	}

	got, _, err := sh.GetRow(ctx, key)
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}
	if s := got[0].String(); s != "ORD-001" {
		t.Errorf("order_ref = %q", s)
	}
	if s := got[1].String(); s != "alice" {
		t.Errorf("customer = %q", s)
	}
	if !got[2].IsNull {
		t.Errorf("note came back as %q, want NULL — NULL and empty must stay distinct", got[2].Bytes)
	}
	// DECIMAL is carried as the text MySQL produced. Parsing and re-rendering it would lose money.
	if s := got[3].String(); s != "19.99" {
		t.Errorf("total = %q, want the exact text MySQL stored", s)
	}
	if u, err := got[4].Uint64(); err != nil || storage.Version(u) != v {
		t.Errorf("version = %v (err %v), want %d", u, err, v)
	}
}

func TestAnArbitraryTableBatchesAndDeletes(t *testing.T) {
	ctx := context.Background()
	sh := ordersShard(ctx, t)
	d := sh.Table().Descriptor()

	refs := []string{"ORD-b1", "ORD-b2", "ORD-b3"}
	keys := make([]schema.Key, 0, len(refs))
	for _, ref := range refs {
		if _, err := sh.PutRow(ctx, storage.Row{
			schema.Str(ref), schema.Str("bob"), schema.Str("note " + ref), schema.Str("1.00"), schema.Uint(0),
		}); err != nil {
			t.Fatalf("PutRow %s: %v", ref, err)
		}
		k, err := d.Key(ref)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}

	// A key that does not exist must be omitted, not returned empty.
	missing, _ := d.Key("ORD-nope")
	rows, _, err := sh.BatchGetRows(ctx, append(keys, missing))
	if err != nil {
		t.Fatalf("BatchGetRows: %v", err)
	}
	if len(rows) != len(refs) {
		t.Fatalf("got %d rows for %d existing keys plus one absent", len(rows), len(refs))
	}
	for _, k := range keys {
		if _, ok := rows[k.String()]; !ok {
			t.Errorf("%s missing from the batch", k)
		}
	}
	if _, ok := rows[missing.String()]; ok {
		t.Error("an absent key came back in the batch; 'missing' and 'present but empty' must differ")
	}

	if _, err := sh.DeleteRow(ctx, keys[0]); err != nil {
		t.Fatalf("DeleteRow: %v", err)
	}
	if _, _, err := sh.GetRow(ctx, keys[0]); err == nil {
		t.Error("a deleted row was still readable")
	}
}

// A string primary key is where the key grammar meets the database. Values containing the key
// separator must name exactly one row, and the row they name must be the right one.
func TestAStringKeyWithSeparatorsNamesOneRow(t *testing.T) {
	ctx := context.Background()
	sh := ordersShard(ctx, t)
	d := sh.Table().Descriptor()

	for _, ref := range []string{"a:b", "a%3Ab", "a%b", "has space", "unicode-🎯"} {
		if _, err := sh.PutRow(ctx, storage.Row{
			schema.Str(ref), schema.Str("carol"), schema.Null(), schema.Str("2.50"), schema.Uint(0),
		}); err != nil {
			t.Fatalf("PutRow %q: %v", ref, err)
		}
	}
	for _, ref := range []string{"a:b", "a%3Ab", "a%b", "has space", "unicode-🎯"} {
		k, err := d.Key(ref)
		if err != nil {
			t.Fatalf("Key(%q): %v", ref, err)
		}
		got, _, err := sh.GetRow(ctx, k)
		if err != nil {
			t.Fatalf("GetRow(%q → %s): %v", ref, k, err)
		}
		if s := got[0].String(); s != ref {
			t.Errorf("key %s read back order_ref %q, want %q — two values named one row", k, s, ref)
		}
	}
}
