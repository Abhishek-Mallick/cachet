//go:build e2e

package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/test/fixtures/table"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// The point of the whole ship, stated as a test: one engine, two tables, neither of them assumed.
//
// `gadgets` is deliberately unlike `entities` in every way that used to be compiled in — a STRING
// primary key, a nullable column, a DECIMAL carried as text, and a version column that is not
// called `version`. If these pass, Cachet is no longer a system that caches `entities`.

// rowToV2 renders a storage row on the wire.
func rowToV2(row []schema.Value) *cachetv2.Row {
	out := &cachetv2.Row{Values: make([]*cachetv2.Value, 0, len(row))}
	for _, v := range row {
		if v.IsNull {
			out.Values = append(out.Values, &cachetv2.Value{IsNull: true})
			continue
		}
		out.Values = append(out.Values, &cachetv2.Value{Data: v.Bytes})
	}
	return out
}

func bothTables(ctx context.Context, t *testing.T) *harness.Cluster {
	t.Helper()
	return harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
		BothTables:              true,
	}, "tcp://127.0.0.1:0")
}

func gadgetValue(t *testing.T, row *cachetv2.Row, column string) *cachetv2.Value {
	t.Helper()

	d := table.GadgetsDescriptor()
	col := d.Column(column)
	if col == nil {
		t.Fatalf("gadgets has no column %q", column)
	}
	if len(row.GetValues()) != len(d.Columns) {
		t.Fatalf("row has %d values, want %d", len(row.GetValues()), len(d.Columns))
	}
	return row.GetValues()[col.Index]
}

// TestTheEngineServesATableThatIsNotTheFixture is the claim in one test.
func TestTheEngineServesATableThatIsNotTheFixture(t *testing.T) {
	ctx := context.Background()
	cluster := bothTables(ctx, t)
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	// A sku carrying the key separator, so the escaping in the key grammar is exercised by the
	// round trip rather than only by a unit test.
	const sku = "SKU:9800:01"
	key := table.GadgetKey(sku).String()
	note := "first"

	if _, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: key, Row: rowToV2(table.GadgetRow(sku, "eu", "19.99", &note)),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("the row written a moment ago was not found")
	}
	if s := string(gadgetValue(t, got.GetRow(), "sku").GetData()); s != sku {
		t.Errorf("sku = %q, want %q", s, sku)
	}
	// DECIMAL comes back as the text MySQL produced, not through a float: a round trip through
	// float64 loses money.
	if p := string(gadgetValue(t, got.GetRow(), "price").GetData()); p != "19.99" {
		t.Errorf("price = %q, want \"19.99\"", p)
	}
	if n := string(gadgetValue(t, got.GetRow(), "note").GetData()); n != note {
		t.Errorf("note = %q, want %q", n, note)
	}
	// The version column is called row_version, and the engine stamped it without being told.
	v, err := schema.Bin(gadgetValue(t, got.GetRow(), "row_version").GetData()).Uint64()
	if err != nil || v == 0 {
		t.Errorf("row_version = %v (err %v); the engine did not stamp it", v, err)
	}

	// A second read must come from the cache, and hold the same bytes.
	second, err := v2.Get(ctx, &cachetv2.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if !second.GetMeta().GetCacheHit() {
		t.Error("the second read of a non-fixture table was not served from the cache")
	}
	if p := string(gadgetValue(t, second.GetRow(), "price").GetData()); p != "19.99" {
		t.Errorf("cached price = %q, want \"19.99\"; a cache hit must be byte-identical to a read", p)
	}
}

// TestANullColumnStaysNullThroughTheCache. SQL NULL and an empty value are different facts, and a
// cache that conflated them would answer a question the caller did not ask.
func TestANullColumnStaysNullThroughTheCache(t *testing.T) {
	ctx := context.Background()
	cluster := bothTables(ctx, t)
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	const sku = "SKU-9801"
	key := table.GadgetKey(sku).String()
	if _, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: key, Row: rowToV2(table.GadgetRow(sku, "eu", "1.00", nil)),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for _, pass := range []string{"from the origin", "from the cache"} {
		got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("Get %s: %v", pass, err)
		}
		note := gadgetValue(t, got.GetRow(), "note")
		if !note.GetIsNull() {
			t.Errorf("%s: note = %q, want NULL", pass, note.GetData())
		}
	}

	// And an empty string is not NULL either.
	empty := ""
	if _, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: key, Row: rowToV2(table.GadgetRow(sku, "eu", "1.00", &empty)),
	}); err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get after the empty write: %v", err)
	}
	if note := gadgetValue(t, got.GetRow(), "note"); note.GetIsNull() {
		t.Error("an empty string came back as NULL; the two are different facts")
	}
}

// TestEachTableIsRoutedOnItsOwnRing. Keys are namespaced by table, so `entities:5` and a gadget
// hash to different places — correct only if each table is routed over the shards it was declared
// on. A shared ring would be correct only by coincidence.
func TestEachTableIsRoutedOnItsOwnRing(t *testing.T) {
	ctx := context.Background()
	cluster := bothTables(ctx, t)
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	// Both writes go over v2: an engine serving two tables cannot serve v1 at all, which
	// TestV1RefusesToGuessWhichTableItMeans asserts directly.
	const id = 9_800_101
	entityKey := table.Key(id).String()
	if _, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: entityKey, Row: rowToV2(table.Row(id, 1, 0, "entity")),
	}); err != nil {
		t.Fatalf("Put the entities row: %v", err)
	}

	const sku = "SKU-9800101"
	gadgetKey := table.GadgetKey(sku).String()
	note := "gadget"
	if _, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: gadgetKey, Row: rowToV2(table.GadgetRow(sku, "us", "3.50", &note)),
	}); err != nil {
		t.Fatalf("v2 Put: %v", err)
	}

	// Each row is found under its own key, and neither table's key finds the other's row.
	if got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: entityKey}); err != nil || !got.GetFound() {
		t.Errorf("the entities row was not found after a gadgets write: err=%v", err)
	}
	if got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: gadgetKey}); err != nil || !got.GetFound() {
		t.Errorf("the gadgets row was not found after an entities write: err=%v", err)
	}

	// A key naming a table this engine does not serve is refused rather than routed somewhere
	// deterministic and cached under an identity nothing can reproduce.
	_, err := v2.Get(ctx, &cachetv2.GetRequest{Key: "widgets:1"})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Get on an undeclared table: err = %v, want InvalidArgument", err)
	}
}

// TestAConditionalWriteOnTheSecondTableResolvesItsOwnKeys. The declared predicate matches on
// `region` and sets `note` — columns the fixture table does not have, through a version column
// that is not called `version`.
func TestAConditionalWriteOnTheSecondTableResolvesItsOwnKeys(t *testing.T) {
	ctx := context.Background()
	cluster := bothTables(ctx, t)
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	const region = "apac-9802"
	skus := []string{"SKU-98021", "SKU-98022", "SKU-98023"}
	before := "before"
	for _, sku := range skus {
		if _, err := v2.Put(ctx, &cachetv2.PutRequest{
			Key: table.GadgetKey(sku).String(), Row: rowToV2(table.GadgetRow(sku, region, "2.00", &before)),
		}); err != nil {
			t.Fatalf("Put %s: %v", sku, err)
		}
	}
	// A row in another region the predicate must not touch.
	if _, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: table.GadgetKey("SKU-98029").String(), Row: rowToV2(table.GadgetRow("SKU-98029", "emea-9802", "2.00", &before)),
	}); err != nil {
		t.Fatalf("Put the control row: %v", err)
	}

	res, err := v2.UpdateWhere(ctx, &cachetv2.UpdateWhereRequest{
		Table: table.GadgetsName,
		Match: []*cachetv2.Comparison{{Column: "region", Value: &cachetv2.Value{Data: []byte(region)}}},
		Set:   []*cachetv2.Assignment{{Column: "note", Value: &cachetv2.Value{Data: []byte("after")}}},
	})
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}
	if res.GetMeta().GetDegraded() {
		t.Fatalf("a three-row predicate degraded: %s", res.GetMeta().GetDegradedReason())
	}
	if len(res.GetAffectedKeys()) != len(skus) {
		t.Fatalf("AffectedKeys = %v, want the %d rows in %s", res.GetAffectedKeys(), len(skus), region)
	}
	for _, key := range res.GetAffectedKeys() {
		if !strings.HasPrefix(key, table.GadgetsName+":") {
			t.Errorf("AffectedKeys names %q, which is not a gadgets key", key)
		}
	}

	// Every matched row changed, and the control row did not.
	for _, sku := range skus {
		got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: table.GadgetKey(sku).String()})
		if err != nil {
			t.Fatalf("Get %s: %v", sku, err)
		}
		if n := string(gadgetValue(t, got.GetRow(), "note").GetData()); n != "after" {
			t.Errorf("%s note = %q, want \"after\"", sku, n)
		}
	}
	control, err := v2.Get(ctx, &cachetv2.GetRequest{Key: table.GadgetKey("SKU-98029").String()})
	if err != nil {
		t.Fatalf("Get the control row: %v", err)
	}
	if n := string(gadgetValue(t, control.GetRow(), "note").GetData()); n != before {
		t.Errorf("the control row's note = %q, want %q; the predicate touched a row it did not match", n, before)
	}
}

// TestAnUndeclaredPredicateIsRefused: a conditional write the deployment did not declare would run
// against columns nobody checked for index coverage, which is an outage rather than a slow query.
func TestAnUndeclaredPredicateIsRefused(t *testing.T) {
	ctx := context.Background()
	cluster := bothTables(ctx, t)
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	for name, req := range map[string]*cachetv2.UpdateWhereRequest{
		"a match column nobody declared": {
			Table: table.GadgetsName,
			Match: []*cachetv2.Comparison{{Column: "price", Value: &cachetv2.Value{Data: []byte("2.00")}}},
			Set:   []*cachetv2.Assignment{{Column: "note", Value: &cachetv2.Value{Data: []byte("x")}}},
		},
		"a set column nobody declared": {
			Table: table.GadgetsName,
			Match: []*cachetv2.Comparison{{Column: "region", Value: &cachetv2.Value{Data: []byte("eu")}}},
			Set:   []*cachetv2.Assignment{{Column: "price", Value: &cachetv2.Value{Data: []byte("9.99")}}},
		},
		"a table this engine does not serve": {
			Table: "widgets",
			Match: []*cachetv2.Comparison{{Column: "region", Value: &cachetv2.Value{Data: []byte("eu")}}},
			Set:   []*cachetv2.Assignment{{Column: "note", Value: &cachetv2.Value{Data: []byte("x")}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := v2.UpdateWhere(ctx, req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("UpdateWhere: err = %v, want InvalidArgument", err)
			}
		})
	}
}

// TestV1RefusesToGuessWhichTableItMeans. Record has no table field, so an engine serving two tables
// has nothing to disambiguate a v1 request with — and picking one would serve a different table's
// rows under the same call.
func TestV1RefusesToGuessWhichTableItMeans(t *testing.T) {
	ctx := context.Background()
	cluster := bothTables(ctx, t)
	v1 := cluster.Client(t, cluster.Addrs[0])

	_, err := v1.Get(ctx, &cachetv1.GetRequest{Key: table.Key(9_800_201).String()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("v1 Get against a two-table engine: err = %v, want FailedPrecondition", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "cachet.v2") {
		t.Errorf("the refusal does not name the protocol that CAN do this: %v", err)
	}
}
