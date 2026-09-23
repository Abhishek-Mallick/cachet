package cachet

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/types/known/durationpb"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// The generic row API.
//
// Cachet's first protocol encoded one table's columns into the protocol itself, which made the SDK
// pleasant for that table and unusable for anyone else's. This is the same client, the same
// session and the same consistency levels, over rows described by the server rather than by the
// wire format.
//
// The descriptors arrive at the handshake. A client that had to be configured with its own copy of
// the schema would be a second copy to keep in step with the first, and the failure mode of two
// copies drifting is reading column 3 as column 4 — silent, and wrong in a way no type checks.

// ErrNoDescriptors reports that the client has no table descriptors.
//
// Either the handshake was skipped, or the server is old enough to speak only cachet.v1. Both are
// answerable by the caller, which is why they are one error rather than a generic failure.
var ErrNoDescriptors = errors.New("cachet: no table descriptors; the server speaks only cachet.v1, or the handshake was skipped")

// Value is one column of one row.
//
// Null is carried separately rather than as an absent Bytes, because SQL NULL and a zero-length
// value are different facts.
//
// Bytes is the canonical representation: decimal text for integers, the raw bytes for everything
// else. Carrying the database's own bytes rather than a parsed value is what makes a cache hit
// byte-identical to a database read.
type Value struct {
	Null  bool
	Bytes []byte
}

// NullValue builds a SQL NULL.
func NullValue() Value { return Value{Null: true} }

// StringValue builds a string value.
func StringValue(s string) Value { return fromSchema(schema.Str(s)) }

// BytesValue builds a byte value.
func BytesValue(b []byte) Value { return fromSchema(schema.Bin(b)) }

// UintValue builds an unsigned integer value.
func UintValue(u uint64) Value { return fromSchema(schema.Uint(u)) }

// IntValue builds a signed integer value.
func IntValue(i int64) Value { return fromSchema(schema.Int(i)) }

// String renders the value. NULL renders as "NULL", which is why a caller distinguishing the two
// should read Null rather than compare strings.
func (v Value) String() string { return v.toSchema().String() }

// Uint64 parses the value as an unsigned integer.
func (v Value) Uint64() (uint64, error) { return v.toSchema().Uint64() }

// Int64 parses the value as a signed integer.
func (v Value) Int64() (int64, error) {
	if v.Null {
		return 0, errors.New("cachet: value is NULL")
	}
	return strconv.ParseInt(string(v.Bytes), 10, 64)
}

func (v Value) toSchema() schema.Value { return schema.Value{IsNull: v.Null, Bytes: v.Bytes} }
func fromSchema(v schema.Value) Value  { return Value{Null: v.IsNull, Bytes: v.Bytes} }

// Column describes one column of a table.
type Column struct {
	Name     string
	Type     string
	Nullable bool
}

// Table is a table descriptor as the server published it.
type Table struct {
	Name          string
	Columns       []Column
	PrimaryKey    []string
	VersionColumn string

	// Fingerprint identifies the row shape. A client that holds a descriptor across a server
	// upgrade can compare it and notice that the shape changed under it.
	Fingerprint string

	index map[string]int
}

// Index returns a column's position in the row, which is the position its value occupies.
func (t *Table) Index(column string) (int, bool) {
	i, ok := t.index[column]
	return i, ok
}

// Key builds a cache key from primary key values, in the table's declared key order.
//
// Values render the way the engine renders them, because a key built two ways is two keys, and two
// keys for one row is one row's value served for another.
func (t *Table) Key(values ...any) (string, error) {
	if len(values) != len(t.PrimaryKey) {
		return "", fmt.Errorf("cachet: %s has %d primary key column(s), got %d value(s)",
			t.Name, len(t.PrimaryKey), len(values))
	}
	k, err := schema.KeyOf(t.Name, values...)
	if err != nil {
		return "", fmt.Errorf("cachet: %w", err)
	}
	return k.String(), nil
}

// Row is one row of one table.
type Row struct {
	table  *Table
	values []Value
}

// NewRow builds a row, checking it against the table's shape.
//
// Checked here rather than at the server so that a mis-shaped row fails where it was built, with
// the table's own column count, instead of arriving as an InvalidArgument from a round trip away.
func NewRow(t *Table, values ...Value) (Row, error) {
	if t == nil {
		return Row{}, errors.New("cachet: nil table")
	}
	if len(values) != len(t.Columns) {
		return Row{}, fmt.Errorf("cachet: %s has %d columns, got %d value(s)",
			t.Name, len(t.Columns), len(values))
	}
	for i, v := range values {
		if v.Null && !t.Columns[i].Nullable {
			return Row{}, fmt.Errorf("cachet: %s.%s is NOT NULL", t.Name, t.Columns[i].Name)
		}
	}
	return Row{table: t, values: values}, nil
}

// Table returns the descriptor this row was read or built against.
func (r Row) Table() *Table { return r.table }

// Values returns the row's columns in declared order.
func (r Row) Values() []Value { return r.values }

// Column reads one column by name.
func (r Row) Column(name string) (Value, error) {
	if r.table == nil {
		return Value{}, ErrNoDescriptors
	}
	i, ok := r.table.Index(name)
	if !ok {
		return Value{}, fmt.Errorf("cachet: %s has no column %q", r.table.Name, name)
	}
	return r.values[i], nil
}

// RowResult is the answer to a generic read.
type RowResult struct {
	// Found is false when the row does not exist, which is an answer rather than an error.
	Found bool
	Row   Row
	Meta  ReadMeta
}

// Tables returns the descriptors the server published, in the order it published them.
func (c *Client) Tables() []*Table {
	out := make([]*Table, 0, len(c.tableOrder))
	for _, name := range c.tableOrder {
		out = append(out, c.tables[name])
	}
	return out
}

// Table returns one published descriptor.
func (c *Client) Table(name string) (*Table, error) {
	if len(c.tables) == 0 {
		return nil, ErrNoDescriptors
	}
	t, ok := c.tables[name]
	if !ok {
		return nil, fmt.Errorf("cachet: the server does not serve a table named %q", name)
	}
	return t, nil
}

// GetRow reads one row generically.
func (c *Client) GetRow(ctx context.Context, key string, opts ...ReadOption) (RowResult, error) {
	if c.apiV2 == nil {
		return RowResult{}, ErrNoDescriptors
	}
	ro := c.readOptions(opts)
	req := &cachetv2.GetRequest{
		Key:     key,
		Level:   cachetv2.ConsistencyLevel(ro.level.Proto()),
		Session: &cachetv2.SessionToken{Watermarks: c.sessionFor(ctx)},
	}
	if ro.level == consistency.Bounded {
		req.StalenessBound = durationpb.New(ro.bound)
	}

	resp, err := c.apiV2.Get(ctx, req)
	if err != nil {
		return RowResult{}, fmt.Errorf("cachet: get row %s: %w", key, err)
	}
	c.observe(resp.GetSession().GetWatermarks())

	out := RowResult{Found: resp.GetFound(), Meta: readMetaV2(resp.GetMeta())}
	if resp.GetFound() {
		row, err := c.rowFromProto(key, resp.GetRow())
		if err != nil {
			return RowResult{}, err
		}
		out.Row = row
	}
	return out, nil
}

// BatchGetRows reads several rows in one call.
//
// It is N independent reads, not a snapshot: two keys may reflect different instants, at every
// level. Cachet caches rows, not transactions.
func (c *Client) BatchGetRows(ctx context.Context, keys []string, opts ...ReadOption) (map[string]Row, ReadMeta, error) {
	if c.apiV2 == nil {
		return nil, ReadMeta{}, ErrNoDescriptors
	}
	ro := c.readOptions(opts)
	req := &cachetv2.BatchGetRequest{
		Keys:    keys,
		Level:   cachetv2.ConsistencyLevel(ro.level.Proto()),
		Session: &cachetv2.SessionToken{Watermarks: c.sessionFor(ctx)},
	}
	if ro.level == consistency.Bounded {
		req.StalenessBound = durationpb.New(ro.bound)
	}

	resp, err := c.apiV2.BatchGet(ctx, req)
	if err != nil {
		return nil, ReadMeta{}, fmt.Errorf("cachet: batch get rows: %w", err)
	}
	c.observe(resp.GetSession().GetWatermarks())

	out := make(map[string]Row, len(resp.GetRows()))
	for key, raw := range resp.GetRows() {
		row, err := c.rowFromProto(key, raw)
		if err != nil {
			return nil, ReadMeta{}, err
		}
		out[key] = row
	}
	return out, readMetaV2(resp.GetMeta()), nil
}

// PutRow writes one row generically.
func (c *Client) PutRow(ctx context.Context, key string, row Row) (WriteResult, error) {
	if c.apiV2 == nil {
		return WriteResult{}, ErrNoDescriptors
	}
	if row.table == nil {
		return WriteResult{}, errors.New("cachet: row was not built with NewRow")
	}

	resp, err := c.apiV2.Put(ctx, &cachetv2.PutRequest{
		Key:     key,
		Row:     rowToProto(row),
		Session: &cachetv2.SessionToken{Watermarks: c.sessionFor(ctx)},
	})
	if err != nil {
		return WriteResult{}, fmt.Errorf("cachet: put row %s: %w", key, err)
	}
	c.observe(resp.GetSession().GetWatermarks())
	return writeResultV2(resp.GetMeta()), nil
}

// rowFromProto binds a wire row to the descriptor for its key's table.
//
// The table comes from the KEY rather than from the response, because the key is what the caller
// asked about. A row bound to a descriptor other than the one its key names would read the right
// bytes under the wrong column names.
func (c *Client) rowFromProto(key string, raw *cachetv2.Row) (Row, error) {
	parsed, err := schema.ParseKey(key)
	if err != nil {
		return Row{}, fmt.Errorf("cachet: %w", err)
	}
	t, err := c.Table(parsed.Table)
	if err != nil {
		return Row{}, err
	}
	if got, want := len(raw.GetValues()), len(t.Columns); got != want {
		return Row{}, fmt.Errorf("cachet: server returned %d values for %s, which has %d columns",
			got, t.Name, want)
	}

	values := make([]Value, len(raw.GetValues()))
	for i, v := range raw.GetValues() {
		if v.GetIsNull() {
			values[i] = Value{Null: true}
			continue
		}
		values[i] = Value{Bytes: v.GetData()}
	}
	return Row{table: t, values: values}, nil
}

// readMetaV2 and writeResultV2 exist so that the two protocols report the same facts to the
// caller. They are deliberately separate from the v1 conversions rather than chained through them:
// a chain would make v2's metadata only as expressive as v1's, which is the opposite of the point.
func readMetaV2(m *cachetv2.ReadMeta) ReadMeta {
	// A level this client cannot name falls back to the default rather than failing the read. The
	// caller already has its answer; refusing to hand it over because the METADATA used an enum
	// value from a newer server would turn a successful read into an error for no benefit.
	served, err := consistency.LevelFromProto(cachetv1.ConsistencyLevel(m.GetLevelServed()))
	if err != nil {
		served = consistency.Session
	}
	return ReadMeta{
		LevelServed:    served,
		Degraded:       m.GetDegraded(),
		DegradedReason: m.GetDegradedReason(),
		CacheHit:       m.GetCacheHit(),
		RowVersion:     m.GetRowVersion(),
		FillVersion:    m.GetFillVersion(),
	}
}

func writeResultV2(m *cachetv2.WriteMeta) WriteResult {
	return WriteResult{
		Version:                 m.GetVersion(),
		Degraded:                m.GetDegraded(),
		DegradedReason:          m.GetDegradedReason(),
		EffectiveStalenessBound: m.GetEffectiveStalenessBound().AsDuration(),
	}
}

func rowToProto(r Row) *cachetv2.Row {
	out := &cachetv2.Row{Values: make([]*cachetv2.Value, 0, len(r.values))}
	for _, v := range r.values {
		if v.Null {
			out.Values = append(out.Values, &cachetv2.Value{IsNull: true})
			continue
		}
		out.Values = append(out.Values, &cachetv2.Value{Data: v.Bytes})
	}
	return out
}

func tableFromProto(d *cachetv2.TableDescriptor) *Table {
	t := &Table{
		Name:          d.GetName(),
		PrimaryKey:    d.GetPrimaryKey(),
		VersionColumn: d.GetVersionColumn(),
		Fingerprint:   d.GetFingerprint(),
		index:         make(map[string]int, len(d.GetColumns())),
	}
	for i, c := range d.GetColumns() {
		t.Columns = append(t.Columns, Column{Name: c.GetName(), Type: c.GetType(), Nullable: c.GetNullable()})
		t.index[c.GetName()] = i
	}
	return t
}
