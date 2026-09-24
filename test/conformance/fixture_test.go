//go:build e2e

package conformance_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/test/fixtures/table"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// The table a conformance cell runs against, and the protocol it speaks to reach it.
//
// The matrix used to be written directly against cachet.v1 and the fixture table, which meant it
// proved the guarantees for one five-column row shape and one wire format. Neither is what Cachet
// claims. The cells are now written against this interface so the same scenarios run over:
//
//	entities/v1   the shipped configuration, byte for byte what the matrix always asserted
//	entities/v2   the same table over the generic protocol
//	gadgets/v2    a STRING primary key, a nullable column, a DECIMAL, and a version column
//	              called row_version
//
// A guarantee that held only for the fixture would be a guarantee about the fixture.

// session is the watermark map both protocols carry. Cells hold one of these rather than either
// protocol's token type, because the guarantee is the map — the message around it is transport.
type session map[string]uint64

type readOutcome struct {
	Found      bool
	Payload    string
	RowVersion uint64
	CacheHit   bool
	Session    session
}

type writeOutcome struct {
	Version uint64
	Session session
}

// fixture is one (table, protocol) pair.
type fixture interface {
	// Name identifies the fixture in test output.
	Name() string

	// BothTables reports whether the harness must declare the second table for this fixture to be
	// servable.
	BothTables() bool

	// Key names the nth row this env owns.
	Key(n uint64) string

	Put(t *testing.T, e *env, key, payload string, sess session) writeOutcome
	Get(t *testing.T, e *env, key string, lv levelSpec, sess session) readOutcome
	Delete(t *testing.T, e *env, key string, sess session) writeOutcome

	// BatchPayloads reads several keys at once and returns the payload of each one present.
	BatchPayloads(t *testing.T, e *env, keys []string, lv levelSpec) map[string]string

	// GetOn reads through a DIFFERENT cluster, for the failover cell.
	GetOn(t *testing.T, c *harness.Cluster, key string, lv levelSpec, sess session) readOutcome

	// WriteBehindTheCache commits straight to the shard, so no invalidation of any kind runs. The
	// staleness cell needs an entry that is genuinely out of date rather than merely old.
	WriteBehindTheCache(t *testing.T, e *env, key, payload string)
}

// ─── entities, over cachet.v1 ───────────────────────────────────────────────────

// entitiesV1 is the shipped configuration: the fixture table over the original protocol.
//
// It stays because cachet.v1 is served until 1.0 and a matrix that stopped executing it would stop
// covering the protocol most existing clients speak.
type entitiesV1 struct{}

func (entitiesV1) Name() string        { return "entities/v1" }
func (entitiesV1) BothTables() bool    { return false }
func (entitiesV1) Key(n uint64) string { return fmt.Sprintf("%s:%d", table.Name, idBase+n) }

func (f entitiesV1) Put(t *testing.T, e *env, key, payload string, sess session) writeOutcome {
	t.Helper()

	resp, err := e.cluster.Client(t, e.cluster.Addrs[0]).Put(context.Background(), &cachetv1.PutRequest{
		Key:     key,
		Record:  &cachetv1.Record{TenantId: 1, Payload: []byte(payload)},
		Session: &cachetv1.SessionToken{Watermarks: sess},
	})
	if err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
	return writeOutcome{Version: resp.GetMeta().GetVersion(), Session: resp.GetSession().GetWatermarks()}
}

func (f entitiesV1) Get(t *testing.T, e *env, key string, lv levelSpec, sess session) readOutcome {
	t.Helper()
	return f.GetOn(t, e.cluster, key, lv, sess)
}

func (entitiesV1) GetOn(t *testing.T, c *harness.Cluster, key string, lv levelSpec, sess session) readOutcome {
	t.Helper()

	req := &cachetv1.GetRequest{
		Key: key, Level: lv.level, Session: &cachetv1.SessionToken{Watermarks: sess},
	}
	if lv.bound > 0 {
		req.StalenessBound = durationProto(lv.bound)
	}
	resp, err := c.Client(t, c.Addrs[0]).Get(context.Background(), req)
	if err != nil {
		t.Fatalf("Get %s at %v: %v", key, lv.level, err)
	}
	return readOutcome{
		Found:      resp.GetFound(),
		Payload:    string(resp.GetRecord().GetPayload()),
		RowVersion: resp.GetMeta().GetRowVersion(),
		CacheHit:   resp.GetMeta().GetCacheHit(),
		Session:    resp.GetSession().GetWatermarks(),
	}
}

func (entitiesV1) Delete(t *testing.T, e *env, key string, sess session) writeOutcome {
	t.Helper()

	resp, err := e.cluster.Client(t, e.cluster.Addrs[0]).Delete(context.Background(), &cachetv1.DeleteRequest{
		Key: key, Session: &cachetv1.SessionToken{Watermarks: sess},
	})
	if err != nil {
		t.Fatalf("Delete %s: %v", key, err)
	}
	return writeOutcome{Version: resp.GetMeta().GetVersion(), Session: resp.GetSession().GetWatermarks()}
}

func (entitiesV1) BatchPayloads(t *testing.T, e *env, keys []string, lv levelSpec) map[string]string {
	t.Helper()

	req := &cachetv1.BatchGetRequest{Keys: keys, Level: lv.level}
	if lv.bound > 0 {
		req.StalenessBound = durationProto(lv.bound)
	}
	resp, err := e.cluster.Client(t, e.cluster.Addrs[0]).BatchGet(context.Background(), req)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	out := make(map[string]string, len(resp.GetRecords()))
	for k, rec := range resp.GetRecords() {
		out[k] = string(rec.GetPayload())
	}
	return out
}

func (entitiesV1) WriteBehindTheCache(t *testing.T, e *env, key, payload string) {
	t.Helper()
	writeRowBehindTheCache(t, e, key, table.Name, func(id string) []schema.Value {
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil {
			t.Fatalf("parse id from %q: %v", key, err)
		}
		return table.Row(n, 1, 0, payload)
	})
}

// ─── any declared table, over cachet.v2 ─────────────────────────────────────────

// genericV2 reaches a table through the generic protocol, which is the only one that can name a
// table at all.
type genericV2 struct {
	name string

	// descriptor is the declared shape, and payloadColumn is the column a cell writes its value
	// into. Everything else in a row is filler the guarantee does not depend on.
	descriptor    *schema.Descriptor
	payloadColumn string

	// keyFor builds the nth key, and rowFor builds a row carrying a payload under that key.
	keyFor func(n uint64) string
	rowFor func(key, payload string) []schema.Value

	// bothTables reports whether this table needs the harness to declare more than the fixture.
	bothTables bool
}

func (f genericV2) Name() string        { return f.name }
func (f genericV2) BothTables() bool    { return f.bothTables }
func (f genericV2) Key(n uint64) string { return f.keyFor(n) }

func (f genericV2) Put(t *testing.T, e *env, key, payload string, sess session) writeOutcome {
	t.Helper()

	resp, err := e.cluster.ClientV2(t, e.cluster.Addrs[0]).Put(context.Background(), &cachetv2.PutRequest{
		Key:     key,
		Row:     valuesToProto(f.rowFor(key, payload)),
		Session: &cachetv2.SessionToken{Watermarks: sess},
	})
	if err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
	return writeOutcome{Version: resp.GetMeta().GetVersion(), Session: resp.GetSession().GetWatermarks()}
}

func (f genericV2) Get(t *testing.T, e *env, key string, lv levelSpec, sess session) readOutcome {
	t.Helper()
	return f.GetOn(t, e.cluster, key, lv, sess)
}

func (f genericV2) GetOn(t *testing.T, c *harness.Cluster, key string, lv levelSpec, sess session) readOutcome {
	t.Helper()

	req := &cachetv2.GetRequest{
		Key:     key,
		Level:   cachetv2.ConsistencyLevel(lv.level),
		Session: &cachetv2.SessionToken{Watermarks: sess},
	}
	if lv.bound > 0 {
		req.StalenessBound = durationProto(lv.bound)
	}
	resp, err := c.ClientV2(t, c.Addrs[0]).Get(context.Background(), req)
	if err != nil {
		t.Fatalf("Get %s at %v: %v", key, lv.level, err)
	}
	return readOutcome{
		Found:      resp.GetFound(),
		Payload:    f.payloadOf(t, resp.GetRow()),
		RowVersion: resp.GetMeta().GetRowVersion(),
		CacheHit:   resp.GetMeta().GetCacheHit(),
		Session:    resp.GetSession().GetWatermarks(),
	}
}

func (f genericV2) Delete(t *testing.T, e *env, key string, sess session) writeOutcome {
	t.Helper()

	resp, err := e.cluster.ClientV2(t, e.cluster.Addrs[0]).Delete(context.Background(), &cachetv2.DeleteRequest{
		Key: key, Session: &cachetv2.SessionToken{Watermarks: sess},
	})
	if err != nil {
		t.Fatalf("Delete %s: %v", key, err)
	}
	return writeOutcome{Version: resp.GetMeta().GetVersion(), Session: resp.GetSession().GetWatermarks()}
}

func (f genericV2) BatchPayloads(t *testing.T, e *env, keys []string, lv levelSpec) map[string]string {
	t.Helper()

	req := &cachetv2.BatchGetRequest{Keys: keys, Level: cachetv2.ConsistencyLevel(lv.level)}
	if lv.bound > 0 {
		req.StalenessBound = durationProto(lv.bound)
	}
	resp, err := e.cluster.ClientV2(t, e.cluster.Addrs[0]).BatchGet(context.Background(), req)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	out := make(map[string]string, len(resp.GetRows()))
	for k, row := range resp.GetRows() {
		out[k] = f.payloadOf(t, row)
	}
	return out
}

func (f genericV2) WriteBehindTheCache(t *testing.T, e *env, key, payload string) {
	t.Helper()
	writeRowBehindTheCache(t, e, key, f.name, func(string) []schema.Value {
		return f.rowFor(key, payload)
	})
}

func (f genericV2) payloadOf(t *testing.T, row *cachetv2.Row) string {
	t.Helper()

	if row == nil {
		return ""
	}
	col := f.descriptor.Column(f.payloadColumn)
	if col == nil {
		t.Fatalf("fixture %s has no column %q", f.name, f.payloadColumn)
	}
	if len(row.GetValues()) != len(f.descriptor.Columns) {
		t.Fatalf("fixture %s: row has %d values, want %d", f.name, len(row.GetValues()), len(f.descriptor.Columns))
	}
	return string(row.GetValues()[col.Index].GetData())
}

// ─── the fixtures the matrix runs ───────────────────────────────────────────────

func entitiesV2Fixture() genericV2 {
	d := table.Descriptor()
	return genericV2{
		name:          table.Name,
		descriptor:    d,
		payloadColumn: "payload",
		keyFor:        func(n uint64) string { return fmt.Sprintf("%s:%d", table.Name, idBase+n) },
		rowFor: func(key, payload string) []schema.Value {
			id, err := strconv.ParseUint(keyValue(key), 10, 64)
			if err != nil {
				panic("conformance: entities key is not an integer: " + key)
			}
			return table.Row(id, 1, 0, payload)
		},
	}
}

// gadgetsV2Fixture is the point of the fixture matrix.
//
// A STRING primary key with separators in it, a nullable column, a DECIMAL carried as text, and a
// version column called row_version. Every guarantee the matrix asserts has to hold here too, or
// it was a guarantee about the fixture table rather than about Cachet.
func gadgetsV2Fixture() genericV2 {
	d := table.GadgetsDescriptor()
	return genericV2{
		name:          table.GadgetsName,
		descriptor:    d,
		payloadColumn: "note",
		bothTables:    true,
		// Separators in the key, so the grammar's escaping is exercised by every cell rather than
		// only by a unit test.
		keyFor: func(n uint64) string { return table.GadgetKey(fmt.Sprintf("SKU:%d:x", idBase+n)).String() },
		rowFor: func(key, payload string) []schema.Value {
			return table.GadgetRow(keyValue(key), "eu", "1.00", &payload)
		},
	}
}

// keyValue returns a single-column key's value, decoded.
func keyValue(key string) string {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		panic("conformance: malformed key " + key)
	}
	if len(parsed.Values) != 1 {
		panic("conformance: composite key where a single-column one was expected: " + key)
	}
	return parsed.Values[0]
}

func valuesToProto(row []schema.Value) *cachetv2.Row {
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

// writeRowBehindTheCache routes a key the way the engine would and writes straight to that shard.
func writeRowBehindTheCache(t *testing.T, e *env, key, tableName string, row func(keyValue string) []schema.Value) {
	t.Helper()

	shardID, err := e.cluster.Router.ShardFor(key)
	if err != nil {
		t.Fatalf("route %s: %v", key, err)
	}
	shard, ok := e.cluster.Shards[shardID]
	if !ok {
		t.Fatalf("no shard %s in the cluster", shardID)
	}
	if _, err := shard.PutRow(context.Background(), tableName, row(keyValue(key))); err != nil {
		t.Fatalf("direct shard write: %v", err)
	}
}
