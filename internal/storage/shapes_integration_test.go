//go:build integration

package storage_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// The shapes a declared table is allowed to have, and the ones it is not.
//
// Everything above storage carries rows opaquely, so this is where a shape either works or does
// not. Each case here is a table somebody would plausibly declare; the ones that are refused are
// refused at BOOT, with a message naming what is wrong, because the alternative is discovering it
// on the request that happens to touch the column.

// shapeTable creates a table, declares it, verifies the declaration against the live schema and
// attaches the statements — the same four steps boot performs, in the same order.
func shapeTable(ctx context.Context, t *testing.T, ddl string, cfg schema.TableConfig, predicates ...storage.PredicateSpec) (*storage.Shard, *schema.Descriptor) {
	t.Helper()

	sh := openTestShard(ctx, t)
	if _, err := storage.DBForTest(sh).ExecContext(ctx, ddl); err != nil {
		t.Fatalf("create %s: %v", cfg.Name, err)
	}

	d, err := schema.NewDescriptor(cfg)
	if err != nil {
		t.Fatalf("NewDescriptor: %v", err)
	}
	cols, indexes, err := storage.Introspect(ctx, storage.DBForTest(sh), cfg.Name)
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	if err := storage.VerifyAgainstLive(d, cols); err != nil {
		t.Fatalf("the declaration does not match the table: %v", err)
	}
	tbl, err := storage.NewTable(d, indexes, predicates...)
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	return sh.WithTables(tbl), d
}

// ─── a composite primary key ────────────────────────────────────────────────────

const shipmentsDDL = `
CREATE TABLE IF NOT EXISTS shipments (
  tenant_id   INT UNSIGNED     NOT NULL,
  reference   VARCHAR(64)      NOT NULL COLLATE utf8mb4_bin,
  carrier     VARCHAR(32)      NOT NULL,
  version     BIGINT UNSIGNED  NOT NULL,
  PRIMARY KEY (tenant_id, reference),
  KEY idx_version (version)
) ENGINE=ROCKSDB`

func shipmentsConfig() schema.TableConfig {
	return schema.TableConfig{
		Name: "shipments", PrimaryKey: []string{"tenant_id", "reference"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "tenant_id", Type: schema.Uint32},
			{Name: "reference", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "carrier", Type: schema.String, Collation: "utf8mb4_0900_ai_ci"},
			{Name: "version", Type: schema.Uint64},
		},
	}
}

// TestACompositeKeyRoundTrips. The key grammar was made composite-capable from day one so that
// supporting it would not be a second breaking change; this is the part that executes.
func TestACompositeKeyRoundTrips(t *testing.T) {
	ctx := context.Background()
	sh, d := shapeTable(ctx, t, shipmentsDDL, shipmentsConfig())

	// A reference containing the key separator, so the escaping is exercised by the round trip.
	key, err := d.Key(uint32(42), "REF:2026:01")
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if want := "shipments:42:REF%3A2026%3A01"; key.String() != want {
		t.Errorf("key = %q, want %q", key, want)
	}

	row := storage.Row{schema.Uint(42), schema.Str("REF:2026:01"), schema.Str("dhl"), schema.Uint(0)}
	if _, err := sh.PutRow(ctx, "shipments", row); err != nil {
		t.Fatalf("PutRow: %v", err)
	}

	got, _, err := sh.GetRow(ctx, key)
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}
	if string(got[1].Bytes) != "REF:2026:01" || string(got[2].Bytes) != "dhl" {
		t.Errorf("row = %v, want the reference and carrier written", got)
	}

	if _, err := sh.DeleteRow(ctx, key); err != nil {
		t.Fatalf("DeleteRow: %v", err)
	}
}

// TestACompositeKeyRefusesWhatItCannotPlan states two limitations rather than hiding them.
//
// Both come from the same place: `WHERE (a,b) IN ((?,?),…)` is a row constructor that MyRocks plans
// differently, so batching and conditional-write key resolution are single-column for now. Refused
// at boot or at the call, never half-done.
func TestACompositeKeyRefusesWhatItCannotPlan(t *testing.T) {
	ctx := context.Background()
	sh, d := shapeTable(ctx, t, shipmentsDDL, shipmentsConfig())

	key, err := d.Key(uint32(42), "REF-BATCH")
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if _, _, err := sh.BatchGetRows(ctx, []schema.Key{key}); err == nil {
		t.Error("BatchGetRows accepted a composite key")
	} else if !strings.Contains(err.Error(), "composite") {
		t.Errorf("the refusal does not say why: %v", err)
	}

	// A declared predicate on a composite-key table is refused at BOOT, because resolving affected
	// rows means selecting them back by key.
	cols, indexes, err := storage.Introspect(ctx, storage.DBForTest(sh), "shipments")
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	if err := storage.VerifyAgainstLive(d, cols); err != nil {
		t.Fatalf("VerifyAgainstLive: %v", err)
	}
	_, err = storage.NewTable(d, indexes, storage.PredicateSpec{Match: []string{"version"}, Set: []string{"carrier"}})
	if err == nil {
		t.Fatal("NewTable accepted a conditional write on a composite-key table")
	}
	if !strings.Contains(err.Error(), "composite") {
		t.Errorf("the refusal does not say why: %v", err)
	}
}

// ─── every type Cachet carries, and every nullable column NULL ──────────────────

const menagerieDDL = `
CREATE TABLE IF NOT EXISTS menagerie (
  id          BIGINT UNSIGNED  NOT NULL,
  amount      DECIMAL(18,4)    NULL,
  happened_at DATETIME(6)      NULL,
  seen_at     TIMESTAMP(3)     NULL,
  doc         JSON             NULL,
  label       VARCHAR(64)      NULL,
  blob_col    LONGBLOB         NULL,
  signed_col  BIGINT           NULL,
  version     BIGINT UNSIGNED  NOT NULL,
  PRIMARY KEY (id),
  KEY idx_version (version)
) ENGINE=ROCKSDB`

func menagerieConfig() schema.TableConfig {
	return schema.TableConfig{
		Name: "menagerie", PrimaryKey: []string{"id"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: schema.Uint64},
			// DECIMAL, DATETIME, TIMESTAMP and JSON are all carried as the TEXT MySQL produced. A
			// round trip through time.Time loses the distinction between what was stored and what a
			// driver chose to format, and DECIMAL through float64 loses money.
			{Name: "amount", Type: schema.Text, Nullable: true},
			{Name: "happened_at", Type: schema.Text, Nullable: true},
			{Name: "seen_at", Type: schema.Text, Nullable: true},
			{Name: "doc", Type: schema.Text, Nullable: true},
			{Name: "label", Type: schema.String, Nullable: true, Collation: "utf8mb4_0900_ai_ci"},
			{Name: "blob_col", Type: schema.Bytes, Nullable: true},
			{Name: "signed_col", Type: schema.Int64, Nullable: true},
			{Name: "version", Type: schema.Uint64},
		},
	}
}

// TestEveryCarriedTypeSurvivesTheRoundTrip. What comes back must be byte-identical to what MySQL
// sent, which is what makes a cache hit indistinguishable from a database read and lets the proxy
// answer over the wire without re-rendering.
func TestEveryCarriedTypeSurvivesTheRoundTrip(t *testing.T) {
	ctx := context.Background()
	sh, d := shapeTable(ctx, t, menagerieDDL, menagerieConfig())

	const (
		amount    = "12345678901234.5678"
		happened  = "2026-02-03 04:05:06.789012"
		seen      = "2026-02-03 04:05:06.789"
		doc       = `{"a": 1, "b": [2, 3]}`
		signedVal = "-9223372036854775808"
	)
	blob := make([]byte, 64*1024)
	for i := range blob {
		blob[i] = byte(i % 251)
	}

	row := storage.Row{
		schema.Uint(1),
		schema.Str(amount), schema.Str(happened), schema.Str(seen), schema.Str(doc),
		schema.Str("a label"), schema.Bin(blob), schema.Str(signedVal),
		schema.Uint(0),
	}
	if _, err := sh.PutRow(ctx, "menagerie", row); err != nil {
		t.Fatalf("PutRow: %v", err)
	}

	key, err := d.Key(uint64(1))
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	got, _, err := sh.GetRow(ctx, key)
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}

	for _, tc := range []struct{ column, want string }{
		{"amount", amount},
		{"happened_at", happened},
		{"signed_col", signedVal},
	} {
		col := d.Column(tc.column)
		if s := string(got[col.Index].Bytes); s != tc.want {
			t.Errorf("%s = %q, want %q", tc.column, s, tc.want)
		}
	}
	// TIMESTAMP(3) and JSON are normalised by MySQL rather than stored verbatim, so what matters is
	// that a second read agrees with the first — which is what a cache entry has to reproduce.
	if s := string(got[d.Column("seen_at").Index].Bytes); !strings.HasPrefix(s, "2026-02-03 04:05:06") {
		t.Errorf("seen_at = %q, want the instant that was written", s)
	}
	if s := string(got[d.Column("doc").Index].Bytes); !strings.Contains(s, `"a"`) {
		t.Errorf("doc = %q, want the document that was written", s)
	}
	if b := got[d.Column("blob_col").Index].Bytes; len(b) != len(blob) || b[1000] != blob[1000] {
		t.Errorf("blob_col came back with %d bytes, want %d", len(b), len(blob))
	}

	// The row must also survive the encoding a cache entry uses, byte for byte.
	encoded, err := d.EncodeRow(got)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	decoded, err := d.DecodeRow(encoded)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	for i := range got {
		if !got[i].Equal(decoded[i]) {
			t.Errorf("column %d did not survive the row encoding: %v vs %v", i, got[i], decoded[i])
		}
	}
}

// TestARowThatIsNullEverywhereItCanBe. NULL and the empty value are different facts, and a row
// where every nullable column is NULL is the case that tells a null bitmap from a convention about
// absent bytes.
func TestARowThatIsNullEverywhereItCanBe(t *testing.T) {
	ctx := context.Background()
	sh, d := shapeTable(ctx, t, menagerieDDL, menagerieConfig())

	row := storage.Row{
		schema.Uint(2),
		schema.Null(), schema.Null(), schema.Null(), schema.Null(),
		schema.Null(), schema.Null(), schema.Null(),
		schema.Uint(0),
	}
	if _, err := sh.PutRow(ctx, "menagerie", row); err != nil {
		t.Fatalf("PutRow: %v", err)
	}

	key, err := d.Key(uint64(2))
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	got, _, err := sh.GetRow(ctx, key)
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}
	for i := range d.Columns {
		col := &d.Columns[i]
		if col.Nullable != got[i].IsNull {
			t.Errorf("%s: IsNull = %t, want %t", col.Name, got[i].IsNull, col.Nullable)
		}
	}

	// And through the encoding, which is where the bitmap either works or does not.
	encoded, err := d.EncodeRow(got)
	if err != nil {
		t.Fatalf("EncodeRow: %v", err)
	}
	decoded, err := d.DecodeRow(encoded)
	if err != nil {
		t.Fatalf("DecodeRow: %v", err)
	}
	for i := range decoded {
		if decoded[i].IsNull != got[i].IsNull {
			t.Errorf("column %d: NULL did not survive the row encoding", i)
		}
	}
}

// TestAnEmptyValueIsNotNullThroughTheDatabase is the same distinction on the other side of the
// wire, and it is the one that was wrong: a non-NULL value with nil bytes was written as SQL NULL,
// and the driver returns nil for an empty string on the way back.
func TestAnEmptyValueIsNotNullThroughTheDatabase(t *testing.T) {
	ctx := context.Background()
	sh, d := shapeTable(ctx, t, menagerieDDL, menagerieConfig())

	row := storage.Row{
		schema.Uint(3),
		schema.Null(), schema.Null(), schema.Null(), schema.Null(),
		// An empty string, and a nil slice that is explicitly NOT null — which is how an empty
		// protobuf bytes field decodes.
		schema.Str(""), schema.Bin(nil), schema.Null(),
		schema.Uint(0),
	}
	if _, err := sh.PutRow(ctx, "menagerie", row); err != nil {
		t.Fatalf("PutRow: %v", err)
	}

	key, err := d.Key(uint64(3))
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	got, _, err := sh.GetRow(ctx, key)
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}
	for _, name := range []string{"label", "blob_col"} {
		v := got[d.Column(name).Index]
		if v.IsNull {
			t.Errorf("%s came back NULL; an empty value is not NULL", name)
		}
		if len(v.Bytes) != 0 {
			t.Errorf("%s = %q, want an empty value", name, v.Bytes)
		}
	}
}

// ─── a generated column ─────────────────────────────────────────────────────────

const receiptsDDL = `
CREATE TABLE IF NOT EXISTS receipts (
  id          BIGINT UNSIGNED  NOT NULL,
  net         DECIMAL(10,2)    NOT NULL,
  tax         DECIMAL(10,2)    NOT NULL,
  gross       DECIMAL(11,2)    AS (net + tax) STORED,
  version     BIGINT UNSIGNED  NOT NULL,
  PRIMARY KEY (id),
  KEY idx_version (version)
) ENGINE=ROCKSDB`

// TestAGeneratedColumnIsLeftToTheDatabase. Declaring one would make Cachet write it, and MySQL
// refuses a write to a generated column — so it is simply not declared, and the proxy's whole-row
// proof then refuses `SELECT *` for this table. That is the honest outcome: the cache does not hold
// the whole row, and it says so.
func TestAGeneratedColumnIsLeftToTheDatabase(t *testing.T) {
	ctx := context.Background()

	cfg := schema.TableConfig{
		Name: "receipts", PrimaryKey: []string{"id"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: schema.Uint64},
			{Name: "net", Type: schema.Text},
			{Name: "tax", Type: schema.Text},
			{Name: "version", Type: schema.Uint64},
		},
	}
	sh, d := shapeTable(ctx, t, receiptsDDL, cfg)

	row := storage.Row{schema.Uint(1), schema.Str("10.00"), schema.Str("2.50"), schema.Uint(0)}
	if _, err := sh.PutRow(ctx, "receipts", row); err != nil {
		t.Fatalf("PutRow: %v", err)
	}

	key, err := d.Key(uint64(1))
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	got, _, err := sh.GetRow(ctx, key)
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}
	if len(got) != len(cfg.Columns) {
		t.Fatalf("read %d columns, want the %d declared", len(got), len(cfg.Columns))
	}

	// The generated column exists in the table and not in the declaration, which is exactly the
	// condition that makes a cache entry less than the whole row.
	live, _, err := storage.Introspect(ctx, storage.DBForTest(sh), "receipts")
	if err != nil {
		t.Fatalf("Introspect: %v", err)
	}
	undeclared := storage.UndeclaredColumns(d, live)
	if len(undeclared) != 1 || undeclared[0] != "gross" {
		t.Errorf("undeclared columns = %v, want [gross]", undeclared)
	}

	// Declaring it is refused by the database on the first write, rather than silently producing
	// wrong rows — checked here so the failure mode is recorded rather than assumed.
	withGross := cfg
	withGross.Columns = append([]schema.ColumnConfig{}, cfg.Columns[:3]...)
	withGross.Columns = append(withGross.Columns,
		schema.ColumnConfig{Name: "gross", Type: schema.Text},
		schema.ColumnConfig{Name: "version", Type: schema.Uint64})
	gd, err := schema.NewDescriptor(withGross)
	if err != nil {
		t.Fatalf("NewDescriptor: %v", err)
	}
	gtbl, err := storage.NewTable(gd, nil)
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	sh2 := openTestShard(ctx, t).WithTables(gtbl)
	_, err = sh2.PutRow(ctx, "receipts", storage.Row{
		schema.Uint(2), schema.Str("1.00"), schema.Str("0.10"), schema.Str("1.10"), schema.Uint(0),
	})
	if err == nil {
		t.Error("writing a generated column succeeded; MySQL should have refused it")
	}
}
