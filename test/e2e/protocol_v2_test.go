//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/pkg/cachet"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// The v2 protocol is the generic-row contract, served from the same engine as v1 for one release.
//
// These tests exist because a dual-serve window only helps if the two protocols are two spellings
// of one state. If a v1 write were invisible to a v2 reader, the "roll your fleet at your own pace"
// promise would be a way to lose writes rather than a way to upgrade safely.

// v2row renders the fixture row the way a v2 client must, from the descriptor's column order.
func v2row(id uint64, tenant, statusCode uint32, payload []byte, version uint64) *cachetv2.Row {
	val := func(v schema.Value) *cachetv2.Value {
		if v.IsNull {
			return &cachetv2.Value{IsNull: true}
		}
		return &cachetv2.Value{Data: v.Bytes}
	}
	return &cachetv2.Row{Values: []*cachetv2.Value{
		val(schema.Uint(id)),
		val(schema.Uint(uint64(tenant))),
		val(schema.Uint(uint64(statusCode))),
		val(schema.Bin(payload)),
		val(schema.Uint(version)),
	}}
}

func v2uint(t *testing.T, v *cachetv2.Value) uint64 {
	t.Helper()
	if v.GetIsNull() {
		t.Fatal("value is NULL where an integer column was expected")
	}
	u, err := schema.Bin(v.GetData()).Uint64()
	if err != nil {
		t.Fatalf("decode integer column: %v", err)
	}
	return u
}

// TestAV1WriteIsVisibleToAV2Reader is the dual-serve claim in its simplest form.
func TestAV1WriteIsVisibleToAV2Reader(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v1 := cluster.Client(t, cluster.Addrs[0])
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_700_001)
	if _, err := v1.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 7, Status: 3, Payload: []byte("written by v1")},
	}); err != nil {
		t.Fatalf("v1 Put: %v", err)
	}

	got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("v2 Get: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("the v2 reader did not find a row the v1 writer had just written")
	}
	values := got.GetRow().GetValues()
	if len(values) != 5 {
		t.Fatalf("row has %d values, want 5", len(values))
	}
	if id := v2uint(t, values[0]); id != 9_700_001 {
		t.Errorf("id = %d, want 9700001", id)
	}
	if tenant := v2uint(t, values[1]); tenant != 7 {
		t.Errorf("tenant_id = %d, want 7", tenant)
	}
	if statusCode := v2uint(t, values[2]); statusCode != 3 {
		t.Errorf("status = %d, want 3", statusCode)
	}
	if p := string(values[3].GetData()); p != "written by v1" {
		t.Errorf("payload = %q, want \"written by v1\"", p)
	}
}

// TestAV2WriteIsVisibleToAV1Reader is the same claim in the other direction, which is the one that
// matters during a rollback: a fleet half-upgraded and then reverted must not lose the writes the
// upgraded half accepted.
func TestAV2WriteIsVisibleToAV1Reader(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v1 := cluster.Client(t, cluster.Addrs[0])
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	const id = 9_700_002
	key := "entities:" + itoa(id)
	if _, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: key, Row: v2row(id, 11, 2, []byte("written by v2"), 0),
	}); err != nil {
		t.Fatalf("v2 Put: %v", err)
	}

	got, err := v1.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("v1 Get: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("the v1 reader did not find a row the v2 writer had just written")
	}
	if got.GetRecord().GetTenantId() != 11 {
		t.Errorf("tenant_id = %d, want 11", got.GetRecord().GetTenantId())
	}
	if got.GetRecord().GetStatus() != 2 {
		t.Errorf("status = %d, want 2", got.GetRecord().GetStatus())
	}
	if p := string(got.GetRecord().GetPayload()); p != "written by v2" {
		t.Errorf("payload = %q, want \"written by v2\"", p)
	}
}

// TestTheV2HandshakePublishesTheTableDescriptor is what lets a v2 client read a Row without being
// configured with its own copy of the schema.
func TestTheV2HandshakePublishesTheTableDescriptor(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	resp, err := v2.Handshake(ctx, &cachetv2.HandshakeRequest{ProtocolVersion: engine.ProtocolVersionV2})
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	if !resp.GetCompatible() {
		t.Fatalf("handshake reported incompatible: %s", resp.GetReason())
	}
	if len(resp.GetTables()) != 1 {
		t.Fatalf("handshake published %d tables, want 1", len(resp.GetTables()))
	}

	d := resp.GetTables()[0]
	if d.GetName() != "entities" {
		t.Errorf("table name = %q, want \"entities\"", d.GetName())
	}
	// The fingerprint is the contract: a client that caches the descriptor must be able to notice
	// that the server's row shape changed under it.
	if d.GetFingerprint() != engine.Fingerprint() {
		t.Errorf("published fingerprint = %q, want %q", d.GetFingerprint(), engine.Fingerprint())
	}
	if got, want := len(d.GetColumns()), 5; got != want {
		t.Fatalf("descriptor has %d columns, want %d", got, want)
	}
	for i, want := range []string{"id", "tenant_id", "status", "payload", "version"} {
		if got := d.GetColumns()[i].GetName(); got != want {
			t.Errorf("column %d = %q, want %q", i, got, want)
		}
	}
	if len(d.GetPrimaryKey()) != 1 || d.GetPrimaryKey()[0] != "id" {
		t.Errorf("primary key = %v, want [id]", d.GetPrimaryKey())
	}
	if d.GetVersionColumn() != "version" {
		t.Errorf("version column = %q, want \"version\"", d.GetVersionColumn())
	}
}

// TestTheV2HandshakeRefusesAnotherProtocol keeps the version negotiation honest: a client that
// announces a protocol this engine does not serve should learn that at the handshake rather than
// from misread rows.
func TestTheV2HandshakeRefusesAnotherProtocol(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	resp, err := v2.Handshake(ctx, &cachetv2.HandshakeRequest{ProtocolVersion: "cachet.v3"})
	if err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	if resp.GetCompatible() {
		t.Fatal("the handshake accepted a protocol version this engine does not serve")
	}
	if resp.GetReason() == "" {
		t.Error("the handshake refused without saying why")
	}
}

// TestAV2PutRefusesARowOfTheWrongShape is the v1 shim's whole job. v1 can only describe the fixture
// table, so a row that is not that shape has no v1 representation; inventing one would write
// columns the caller never sent.
func TestAV2PutRefusesARowOfTheWrongShape(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	_, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: "entities:" + itoa(9_700_003),
		Row: &cachetv2.Row{Values: []*cachetv2.Value{{Data: schema.Uint(9_700_003).Bytes}}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put of a one-column row: err = %v, want InvalidArgument", err)
	}
}

// TestAV2PutRefusesAKeyThatDoesNotNameItsRow guards the one silent corruption the split of key and
// row makes possible: a row stored under a key that does not identify it is findable only by a key
// nobody would construct.
func TestAV2PutRefusesAKeyThatDoesNotNameItsRow(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	_, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: "entities:" + itoa(9_700_004),
		Row: v2row(9_700_005, 1, 0, []byte("mismatched"), 0),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Put of a mismatched key: err = %v, want InvalidArgument", err)
	}
}

// TestAV2ConditionalWriteRefusesAPredicateV1CannotExpress checks that the shim narrows loudly.
// Matching on the nearest available column instead would change which rows a write touched.
func TestAV2ConditionalWriteRefusesAPredicateV1CannotExpress(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	cases := map[string]*cachetv2.UpdateWhereRequest{
		"a column the fixture table has but v1 cannot match on": {
			Match: []*cachetv2.Comparison{
				{Column: "tenant_id", Value: &cachetv2.Value{Data: schema.Uint(1).Bytes}},
				{Column: "payload", Value: &cachetv2.Value{Data: []byte("x")}},
			},
			Set: []*cachetv2.Assignment{{Column: "status", Value: &cachetv2.Value{Data: schema.Uint(1).Bytes}}},
		},
		"a match missing tenant_id": {
			Match: []*cachetv2.Comparison{
				{Column: "status", Value: &cachetv2.Value{Data: schema.Uint(0).Bytes}},
			},
			Set: []*cachetv2.Assignment{{Column: "status", Value: &cachetv2.Value{Data: schema.Uint(1).Bytes}}},
		},
		"an assignment to a column other than status": {
			Match: []*cachetv2.Comparison{
				{Column: "tenant_id", Value: &cachetv2.Value{Data: schema.Uint(1).Bytes}},
				{Column: "status", Value: &cachetv2.Value{Data: schema.Uint(0).Bytes}},
			},
			Set: []*cachetv2.Assignment{{Column: "payload", Value: &cachetv2.Value{Data: []byte("x")}}},
		},
		"a table this engine does not serve": {
			Table: "orders",
			Match: []*cachetv2.Comparison{
				{Column: "tenant_id", Value: &cachetv2.Value{Data: schema.Uint(1).Bytes}},
				{Column: "status", Value: &cachetv2.Value{Data: schema.Uint(0).Bytes}},
			},
			Set: []*cachetv2.Assignment{{Column: "status", Value: &cachetv2.Value{Data: schema.Uint(1).Bytes}}},
		},
	}

	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := v2.UpdateWhere(ctx, req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("UpdateWhere: err = %v, want InvalidArgument", err)
			}
		})
	}
}

// TestAV2ConditionalWriteThatV1CanExpressStillRuns is the other half: narrowing loudly is only
// correct if the predicates that DO fit are served.
func TestAV2ConditionalWriteThatV1CanExpressStillRuns(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v1 := cluster.Client(t, cluster.Addrs[0])
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	const (
		id     = 9_700_006
		tenant = 4_242
	)
	key := "entities:" + itoa(id)
	if _, err := v1.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: tenant, Status: 1, Payload: []byte("conditional")},
	}); err != nil {
		t.Fatalf("v1 Put: %v", err)
	}

	resp, err := v2.UpdateWhere(ctx, &cachetv2.UpdateWhereRequest{
		Table: "entities",
		Match: []*cachetv2.Comparison{
			{Column: "tenant_id", Value: &cachetv2.Value{Data: schema.Uint(tenant).Bytes}},
			{Column: "status", Value: &cachetv2.Value{Data: schema.Uint(1).Bytes}},
		},
		Set: []*cachetv2.Assignment{{Column: "status", Value: &cachetv2.Value{Data: schema.Uint(9).Bytes}}},
	})
	if err != nil {
		t.Fatalf("v2 UpdateWhere: %v", err)
	}
	if resp.GetMatched() == 0 {
		t.Fatal("the conditional write matched no rows")
	}

	// STRONG so the assertion is about the database rather than about whether the cache was
	// invalidated — cache invalidation on conditional writes has its own tests.
	got, err := v1.Get(ctx, &cachetv1.GetRequest{
		Key: key, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG,
	})
	if err != nil {
		t.Fatalf("v1 Get: %v", err)
	}
	if got.GetRecord().GetStatus() != 9 {
		t.Errorf("status after the conditional write = %d, want 9", got.GetRecord().GetStatus())
	}
}

// TestAV2DeleteIsVisibleToAV1Reader closes the loop on the third write verb.
func TestAV2DeleteIsVisibleToAV1Reader(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v1 := cluster.Client(t, cluster.Addrs[0])
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	const id = 9_700_007
	key := "entities:" + itoa(id)
	if _, err := v1.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("doomed")},
	}); err != nil {
		t.Fatalf("v1 Put: %v", err)
	}
	// Warm the cache first, so the delete has to invalidate rather than merely miss.
	if _, err := v1.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("warming Get: %v", err)
	}

	del, err := v2.Delete(ctx, &cachetv2.DeleteRequest{Key: key})
	if err != nil {
		t.Fatalf("v2 Delete: %v", err)
	}
	if !del.GetExisted() {
		t.Error("the v2 delete reported the row did not exist")
	}

	got, err := v1.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("v1 Get: %v", err)
	}
	if got.GetFound() {
		t.Error("the v1 reader still found a row the v2 client had deleted")
	}
}

// TestAV2BatchGetReadsWhatV1Wrote covers the last verb, and with it the whole surface.
func TestAV2BatchGetReadsWhatV1Wrote(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	v1 := cluster.Client(t, cluster.Addrs[0])
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	keys := make([]string, 0, 3)
	for i := range 3 {
		key := "entities:" + itoa(uint64(9_700_010+i))
		keys = append(keys, key)
		if _, err := v1.Put(ctx, &cachetv1.PutRequest{
			Key: key, Record: &cachetv1.Record{TenantId: uint32(i + 1), Payload: []byte("batch")},
		}); err != nil {
			t.Fatalf("v1 Put %s: %v", key, err)
		}
	}

	got, err := v2.BatchGet(ctx, &cachetv2.BatchGetRequest{Keys: keys})
	if err != nil {
		t.Fatalf("v2 BatchGet: %v", err)
	}
	if len(got.GetRows()) != len(keys) {
		t.Fatalf("BatchGet returned %d rows, want %d", len(got.GetRows()), len(keys))
	}
	for i, key := range keys {
		row, ok := got.GetRows()[key]
		if !ok {
			t.Fatalf("BatchGet did not return %s", key)
		}
		if tenant := v2uint(t, row.GetValues()[1]); tenant != uint64(i+1) {
			t.Errorf("%s tenant_id = %d, want %d", key, tenant, i+1)
		}
	}
}

// ─── the SDK's generic row API ──────────────────────────────────────────────────

func dialSDK(t *testing.T, cluster *harness.Cluster) *cachet.Client {
	t.Helper()
	c, err := cachet.Dial(context.Background(), cluster.Addrs[0].String())
	if err != nil {
		t.Fatalf("cachet.Dial: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

// TestTheSDKLearnsTheTableFromTheHandshake is what replaces configuring the client with its own
// copy of the schema. Two copies of a schema are two things to keep in step, and the failure mode
// of their drifting is reading one column's bytes under another column's name.
func TestTheSDKLearnsTheTableFromTheHandshake(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := dialSDK(t, cluster)

	tables := c.Tables()
	if len(tables) != 1 {
		t.Fatalf("the SDK learned %d tables, want 1", len(tables))
	}
	table, err := c.Table("entities")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}
	if table.Fingerprint != engine.Fingerprint() {
		t.Errorf("fingerprint = %q, want %q", table.Fingerprint, engine.Fingerprint())
	}
	if i, ok := table.Index("payload"); !ok || i != 3 {
		t.Errorf("payload index = %d (found %t), want 3", i, ok)
	}

	// The key the SDK builds must be the key the engine has always produced, or the hash ring
	// places the entry somewhere the rest of the system does not look for it.
	key, err := table.Key(uint64(9_700_020))
	if err != nil {
		t.Fatalf("Key: %v", err)
	}
	if want := "entities:9700020"; key != want {
		t.Errorf("Key = %q, want %q", key, want)
	}
}

// TestTheSDKRoundTripsAGenericRow is the generic API end to end, through the same session and the
// same consistency levels as the typed one.
func TestTheSDKRoundTripsAGenericRow(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := dialSDK(t, cluster)

	table, err := c.Table("entities")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}
	const id = 9_700_021
	key, err := table.Key(uint64(id))
	if err != nil {
		t.Fatalf("Key: %v", err)
	}

	row, err := cachet.NewRow(table,
		cachet.UintValue(id),
		cachet.UintValue(5),
		cachet.UintValue(1),
		cachet.BytesValue([]byte("generic")),
		cachet.UintValue(0),
	)
	if err != nil {
		t.Fatalf("NewRow: %v", err)
	}
	if _, err := c.PutRow(ctx, key, row); err != nil {
		t.Fatalf("PutRow: %v", err)
	}

	got, err := c.GetRow(ctx, key)
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}
	if !got.Found {
		t.Fatal("GetRow did not find the row it had just written")
	}
	payload, err := got.Row.Column("payload")
	if err != nil {
		t.Fatalf("Column: %v", err)
	}
	if payload.String() != "generic" {
		t.Errorf("payload = %q, want \"generic\"", payload)
	}
	tenant, err := got.Row.Column("tenant_id")
	if err != nil {
		t.Fatalf("Column: %v", err)
	}
	if u, err := tenant.Uint64(); err != nil || u != 5 {
		t.Errorf("tenant_id = %v (err %v), want 5", u, err)
	}
}

// TestTheSDKsTypedAndGenericAPIsAgree is the property that makes the migration incremental: one
// client, one session, and a caller can move call sites over one at a time.
func TestTheSDKsTypedAndGenericAPIsAgree(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := dialSDK(t, cluster)

	table, err := c.Table("entities")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}
	const id = 9_700_022
	key, err := table.Key(uint64(id))
	if err != nil {
		t.Fatalf("Key: %v", err)
	}

	if _, err := c.Put(ctx, key, cachet.Record{TenantID: 6, Status: 2, Payload: []byte("typed")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	generic, err := c.GetRow(ctx, key)
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}
	typed, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !generic.Found || !typed.Found {
		t.Fatalf("Found: generic %t, typed %t — both should be true", generic.Found, typed.Found)
	}

	payload, err := generic.Row.Column("payload")
	if err != nil {
		t.Fatalf("Column: %v", err)
	}
	if payload.String() != string(typed.Record.Payload) {
		t.Errorf("payload: generic %q, typed %q", payload, typed.Record.Payload)
	}
	statusValue, err := generic.Row.Column("status")
	if err != nil {
		t.Fatalf("Column: %v", err)
	}
	got, err := statusValue.Uint64()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if got != uint64(typed.Record.Status) {
		t.Errorf("status: generic %d, typed %d", got, typed.Record.Status)
	}
}

// TestTheSDKRefusesARowOfTheWrongShapeBeforeSending keeps a mis-shaped row failing where it was
// built, with the table's own column count, rather than as an InvalidArgument a round trip away.
func TestTheSDKRefusesARowOfTheWrongShapeBeforeSending(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := dialSDK(t, cluster)

	table, err := c.Table("entities")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}
	if _, err := cachet.NewRow(table, cachet.UintValue(1)); err == nil {
		t.Fatal("NewRow accepted a one-value row for a five-column table")
	}
	if _, err := cachet.NewRow(table,
		cachet.UintValue(1), cachet.UintValue(1), cachet.UintValue(1),
		cachet.NullValue(), cachet.UintValue(0),
	); err == nil {
		t.Fatal("NewRow accepted a NULL in a NOT NULL column")
	}
}
