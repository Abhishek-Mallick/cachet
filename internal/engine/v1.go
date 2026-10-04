package engine

import (
	"context"
	"fmt"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// cachet.v1, as a shim over the engine's own row operations.
//
// v1 spells one table's columns into the protocol: Record has tenant_id, status and payload as
// FIELDS, and UpdateWhereRequest has tenant_id, match_status and set_status. It therefore has
// nowhere to put a table name and cannot describe anybody else's table. It is served until 1.0 so
// that a client built before cachet.v2 keeps working across the upgrade rather than discovering the
// change as a connection error (ADR 0007).
//
// Every refusal here is the same refusal: v1 cannot express this, and inventing a meaning for it
// would be a wrong answer dressed as a working one.

// legacyShape is the row shape v1 can describe.
//
// The NAME is not part of it. A deployment whose table is called `widgets` with these five columns
// is served over v1 correctly; one with different columns is refused, naming what did not fit.
var legacyShape = []schema.ColumnConfig{
	{Name: "id", Type: schema.Uint64},
	{Name: "tenant_id", Type: schema.Uint32},
	{Name: "status", Type: schema.Uint8},
	{Name: "payload", Type: schema.Bytes},
	{Name: "version", Type: schema.Uint64},
}

// v1Table resolves the one table v1 can be talking about, and checks v1 can describe it.
func (e *Engine) v1Table() (*table, error) {
	t, err := e.soleTable()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := checkLegacyShape(t.d); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return t, nil
}

// checkLegacyShape refuses a table cachet.v1 has no way to describe.
//
// Column by column rather than by comparing fingerprints, because a fingerprint mismatch says only
// that something differs. An operator reading this needs to know which column and how.
func checkLegacyShape(d *schema.Descriptor) error {
	if len(d.Columns) != len(legacyShape) {
		return fmt.Errorf("cachet.v1 describes a five-column row (id, tenant_id, status, payload, version); "+
			"table %q declares %d columns. Use cachet.v2", d.Name, len(d.Columns))
	}
	for i, want := range legacyShape {
		got := &d.Columns[i]
		if got.Name != want.Name || got.Type != want.Type || got.Nullable {
			return fmt.Errorf("cachet.v1 describes column %d as %s %s (NOT NULL); table %q declares %s %s (nullable=%t). Use cachet.v2",
				i, want.Name, want.Type, d.Name, got.Name, got.Type, got.Nullable)
		}
	}
	if len(d.PrimaryKey) != 1 || d.PrimaryKey[0].Name != "id" {
		return fmt.Errorf("cachet.v1 names rows by a single `id` column; table %q does not. Use cachet.v2", d.Name)
	}
	if d.VersionColumn.Name != "version" {
		return fmt.Errorf("cachet.v1 has no field for a version column called %q; table %q. Use cachet.v2",
			d.VersionColumn.Name, d.Name)
	}
	return nil
}

// v1Key builds the key for a v1 request, which can only name a row by integer id.
func (e *Engine) v1Key(raw string) (schema.Key, *table, error) {
	t, err := e.v1Table()
	if err != nil {
		return schema.Key{}, nil, err
	}
	key, err := schema.ParseKey(raw)
	if err != nil {
		return schema.Key{}, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if key.Table != t.d.Name {
		return schema.Key{}, nil, status.Errorf(codes.InvalidArgument,
			"engine: unknown table %q in key %q", key.Table, raw)
	}
	if len(key.Values) != 1 {
		return schema.Key{}, nil, status.Errorf(codes.InvalidArgument, "engine: key %q must be <table>:<id>", raw)
	}
	if _, err := schema.Str(key.Values[0]).Uint64(); err != nil {
		return schema.Key{}, nil, status.Errorf(codes.InvalidArgument, "engine: key %q has an invalid id", raw)
	}
	return key, t, nil
}

// Get reads one row.
func (e *Engine) Get(ctx context.Context, req *cachetv1.GetRequest) (*cachetv1.GetResponse, error) {
	reqmt, err := consistency.RequirementFromProto(req.GetLevel(), req.GetStalenessBound())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key, _, err := e.v1Key(req.GetKey())
	if err != nil {
		return nil, err
	}

	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)
	res, err := e.GetRow(ctx, key, reqmt, token)
	if err != nil {
		return nil, e.rpcError(ctx, "get", err)
	}

	out := &cachetv1.GetResponse{Found: res.Found, Session: token.Proto()}
	if res.CacheHit {
		out.Meta = cacheHitMeta(reqmt.Level, res.Entry)
	} else {
		out.Meta = readMeta(reqmt.Level, res.RowVersion, res.FillVersion)
	}
	if res.FromGutter {
		// cachet.v1's ReadMeta has no field for a duration, so the bound goes in the reason. A v1
		// caller still learns that this answer is weaker than the level it asked for, which is the
		// part it can act on.
		out.Meta.Degraded = true
		out.Meta.DegradedReason = fmt.Sprintf("%s (bounded by %s)", gutterReason, e.gutterTTL)
	}
	if res.Found {
		rec, err := rowToV1Record(res.Row)
		if err != nil {
			return nil, e.rpcError(ctx, "get", err)
		}
		out.Record = rec
	}
	return out, nil
}

// BatchGet reads several rows.
func (e *Engine) BatchGet(ctx context.Context, req *cachetv1.BatchGetRequest) (*cachetv1.BatchGetResponse, error) {
	reqmt, err := consistency.RequirementFromProto(req.GetLevel(), req.GetStalenessBound())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	keys := make([]schema.Key, 0, len(req.GetKeys()))
	seen := make(map[string]struct{}, len(req.GetKeys()))
	for _, raw := range req.GetKeys() {
		key, _, err := e.v1Key(raw)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[key.String()]; dup {
			continue
		}
		seen[key.String()] = struct{}{}
		keys = append(keys, key)
	}

	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)
	rows, newest, err := e.BatchGetRows(ctx, keys, token)
	if err != nil {
		return nil, e.rpcError(ctx, "batch get", err)
	}

	out := make(map[string]*cachetv1.Record, len(rows))
	for key, row := range rows {
		rec, err := rowToV1Record(row)
		if err != nil {
			return nil, e.rpcError(ctx, "batch get", err)
		}
		out[key] = rec
	}
	return &cachetv1.BatchGetResponse{
		Records: out,
		Meta:    readMeta(reqmt.Level, 0, newest),
		Session: token.Proto(),
	}, nil
}

// Put writes one row.
func (e *Engine) Put(ctx context.Context, req *cachetv1.PutRequest) (*cachetv1.PutResponse, error) {
	key, _, err := e.v1Key(req.GetKey())
	if err != nil {
		return nil, err
	}
	if req.GetRecord() == nil {
		return nil, status.Error(codes.InvalidArgument, "engine: put requires a record")
	}
	row, err := v1RecordToRow(key, req.GetRecord())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)
	version, err := e.PutRow(ctx, key, row, token)
	if err != nil {
		return nil, e.rpcError(ctx, "put", err)
	}
	return &cachetv1.PutResponse{
		Meta:    &cachetv1.WriteMeta{Version: uint64(version)},
		Session: token.Proto(),
	}, nil
}

// Delete removes one row.
func (e *Engine) Delete(ctx context.Context, req *cachetv1.DeleteRequest) (*cachetv1.DeleteResponse, error) {
	key, _, err := e.v1Key(req.GetKey())
	if err != nil {
		return nil, err
	}

	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)
	existed, version, err := e.DeleteRow(ctx, key, token)
	if err != nil {
		return nil, e.rpcError(ctx, "delete", err)
	}
	return &cachetv1.DeleteResponse{
		Existed: existed,
		Meta:    &cachetv1.WriteMeta{Version: uint64(version)},
		Session: token.Proto(),
	}, nil
}

// degradedReason is the exact wording CONSISTENCY.md §5 promises a caller will see.
const degradedReason = "predicate exceeded max_affected_keys"

// UpdateWhere applies the one conditional-write shape v1 can express.
func (e *Engine) UpdateWhere(ctx context.Context, req *cachetv1.UpdateWhereRequest) (*cachetv1.UpdateWhereResponse, error) {
	t, err := e.v1Table()
	if err != nil {
		return nil, err
	}

	// The same guard Put carries, for the same reason: converting silently would wrap, a client
	// sending 300 would store 44, and the row would differ from what the caller believes it wrote.
	match, err := statusToUint8(req.GetMatchStatus(), "match_status")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	set, err := statusToUint8(req.GetSetStatus(), "set_status")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	token := consistency.TokenFromProto(req.GetSession(), e.maxSessionShards)
	out, err := e.UpdateRowsWhere(ctx, t.d.Name,
		[]storage.ColumnValue{
			{Column: "tenant_id", Value: schema.Uint(uint64(req.GetTenantId()))},
			{Column: "status", Value: schema.Uint(uint64(match))},
		},
		[]storage.ColumnValue{{Column: "status", Value: schema.Uint(uint64(set))}},
		token)
	if err != nil {
		return nil, e.rpcError(ctx, "update where", err)
	}

	meta := &cachetv1.WriteMeta{Version: uint64(out.Newest)}
	if out.Degraded {
		meta.Degraded = true
		meta.DegradedReason = degradedReason
		meta.EffectiveStalenessBound = durationpb.New(e.cdcLagBound)
	}
	return &cachetv1.UpdateWhereResponse{
		Matched:      out.Matched,
		AffectedKeys: out.AffectedKeys,
		Meta:         meta,
		Session:      token.Proto(),
	}, nil
}

// statusToUint8 narrows a proto status field, rejecting anything that would wrap.
func statusToUint8(v uint32, field string) (uint8, error) {
	if v > math.MaxUint8 {
		return 0, fmt.Errorf("engine: %s %d does not fit in a uint8", field, v)
	}
	return uint8(v), nil
}

// rowToV1Record reads a legacy-shaped row as the v1 record.
func rowToV1Record(row storage.Row) (*cachetv1.Record, error) {
	if len(row) != len(legacyShape) {
		return nil, fmt.Errorf("engine: row has %d columns, and cachet.v1 describes %d", len(row), len(legacyShape))
	}
	id, err := row[0].Uint64()
	if err != nil {
		return nil, err
	}
	tenant, err := row[1].Uint64()
	if err != nil {
		return nil, err
	}
	st, err := row[2].Uint64()
	if err != nil {
		return nil, err
	}
	version, err := row[4].Uint64()
	if err != nil {
		return nil, err
	}
	if tenant > math.MaxUint32 || st > math.MaxUint8 {
		return nil, fmt.Errorf("engine: row does not fit the shape cachet.v1 describes")
	}
	return &cachetv1.Record{
		Id:       id,
		TenantId: uint32(tenant),
		Status:   uint32(st),
		Payload:  row[3].Bytes,
		Version:  version,
	}, nil
}

// v1RecordToRow renders the v1 record as a row.
//
// The id comes from the KEY, not from the record. That is v1's own contract — its callers have
// always been able to leave Record.id unset — so the shim honours it here rather than making the
// generic path, where key and row must agree, accept a disagreement for everyone.
//
// The version is a placeholder: storage issues it. Versions are Cachet's to hand out, and a write
// carrying its own would order itself against the engine's.
func v1RecordToRow(key schema.Key, rec *cachetv1.Record) (storage.Row, error) {
	st, err := statusToUint8(rec.GetStatus(), "status")
	if err != nil {
		return nil, err
	}
	return storage.Row{
		schema.Str(key.Values[0]),
		schema.Uint(uint64(rec.GetTenantId())),
		schema.Uint(uint64(st)),
		schema.Bin(rec.GetPayload()),
		schema.Uint(0),
	}, nil
}
