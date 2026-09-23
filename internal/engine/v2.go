package engine

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// ProtocolVersionV2 is the generic-row wire contract.
//
// Served alongside cachet.v1 for one release. v1 encodes one table's schema into the protocol
// itself, so it cannot describe anybody else's table; it stays until 1.0 so that a client built
// against v0.1.0 keeps working across the upgrade rather than discovering the change as a
// connection error (ADR 0007).
const ProtocolVersionV2 = "cachet.v2"

// V2 serves cachet.v2 from the same engine that serves cachet.v1.
//
// A thin adapter rather than a second engine: routing, leases, admission, invalidation and the
// consistency rules are the engine's, and having two of any of them is how two protocols come to
// mean two different things by SESSION.
type V2 struct {
	cachetv2.UnimplementedCacheServiceServer
	e *Engine
}

// NewV2 wraps an engine in the v2 service.
func NewV2(e *Engine) *V2 { return &V2{e: e} }

// Handshake reports compatibility and publishes the table descriptors.
//
// The descriptors are what let a client read a Row without being configured with a copy of the
// server's schema — two copies of a schema being two things to drift.
func (v *V2) Handshake(_ context.Context, req *cachetv2.HandshakeRequest) (*cachetv2.HandshakeResponse, error) {
	if p := req.GetProtocolVersion(); p != "" && p != ProtocolVersionV2 {
		return &cachetv2.HandshakeResponse{
			ServerVersion:   v.e.version,
			ProtocolVersion: ProtocolVersionV2,
			Compatible:      false,
			Reason: fmt.Sprintf("client speaks %s, this engine speaks %s on this service",
				p, ProtocolVersionV2),
		}, nil
	}

	return &cachetv2.HandshakeResponse{
		ServerVersion:   v.e.version,
		ProtocolVersion: ProtocolVersionV2,
		Compatible:      true,
		Tables:          tablesToProto(v.e.Tables()),
	}, nil
}

// Get reads one row.
func (v *V2) Get(ctx context.Context, req *cachetv2.GetRequest) (*cachetv2.GetResponse, error) {
	reqmt, err := consistency.RequirementFromProto(levelToV1(req.GetLevel()), req.GetStalenessBound())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key, err := v.key(req.GetKey())
	if err != nil {
		return nil, err
	}

	token := consistency.TokenFromProto(sessionToV1(req.GetSession()), v.e.maxSessionShards)
	res, err := v.e.GetRow(ctx, key, reqmt, token)
	if err != nil {
		return nil, v.e.rpcError(ctx, "get", err)
	}

	out := &cachetv2.GetResponse{
		Found:   res.Found,
		Meta:    v2ReadMeta(reqmt.Level, res),
		Session: sessionToV2(token.Proto()),
	}
	if res.Found {
		out.Row = rowToProto(res.Row)
	}
	return out, nil
}

// BatchGet reads several rows.
func (v *V2) BatchGet(ctx context.Context, req *cachetv2.BatchGetRequest) (*cachetv2.BatchGetResponse, error) {
	reqmt, err := consistency.RequirementFromProto(levelToV1(req.GetLevel()), req.GetStalenessBound())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	keys := make([]schema.Key, 0, len(req.GetKeys()))
	seen := make(map[string]struct{}, len(req.GetKeys()))
	for _, raw := range req.GetKeys() {
		key, err := v.key(raw)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[key.String()]; dup {
			continue
		}
		seen[key.String()] = struct{}{}
		keys = append(keys, key)
	}

	token := consistency.TokenFromProto(sessionToV1(req.GetSession()), v.e.maxSessionShards)
	rows, newest, err := v.e.BatchGetRows(ctx, keys, token)
	if err != nil {
		return nil, v.e.rpcError(ctx, "batch get", err)
	}

	out := make(map[string]*cachetv2.Row, len(rows))
	for key, row := range rows {
		out[key] = rowToProto(row)
	}
	return &cachetv2.BatchGetResponse{
		Rows: out,
		Meta: &cachetv2.ReadMeta{
			LevelServed: cachetv2.ConsistencyLevel(reqmt.Level.Proto()),
			FillVersion: uint64(newest),
		},
		Session: sessionToV2(token.Proto()),
	}, nil
}

// Put writes one row.
func (v *V2) Put(ctx context.Context, req *cachetv2.PutRequest) (*cachetv2.PutResponse, error) {
	key, err := v.key(req.GetKey())
	if err != nil {
		return nil, err
	}
	row, err := rowFromProto(req.GetRow())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	token := consistency.TokenFromProto(sessionToV1(req.GetSession()), v.e.maxSessionShards)
	version, err := v.e.PutRow(ctx, key, row, token)
	if err != nil {
		return nil, v.e.rpcError(ctx, "put", err)
	}
	return &cachetv2.PutResponse{
		Meta:    &cachetv2.WriteMeta{Version: uint64(version)},
		Session: sessionToV2(token.Proto()),
	}, nil
}

// Delete removes one row.
func (v *V2) Delete(ctx context.Context, req *cachetv2.DeleteRequest) (*cachetv2.DeleteResponse, error) {
	key, err := v.key(req.GetKey())
	if err != nil {
		return nil, err
	}

	token := consistency.TokenFromProto(sessionToV1(req.GetSession()), v.e.maxSessionShards)
	existed, version, err := v.e.DeleteRow(ctx, key, token)
	if err != nil {
		return nil, v.e.rpcError(ctx, "delete", err)
	}
	return &cachetv2.DeleteResponse{
		Existed: existed,
		Meta:    &cachetv2.WriteMeta{Version: uint64(version)},
		Session: sessionToV2(token.Proto()),
	}, nil
}

// UpdateWhere applies a declared conditional write.
func (v *V2) UpdateWhere(ctx context.Context, req *cachetv2.UpdateWhereRequest) (*cachetv2.UpdateWhereResponse, error) {
	if req.GetTable() == "" {
		return nil, status.Error(codes.InvalidArgument, "engine: a conditional write must name its table")
	}
	match, err := columnValues(req.GetMatch())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	set, err := assignments(req.GetSet())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	token := consistency.TokenFromProto(sessionToV1(req.GetSession()), v.e.maxSessionShards)
	out, err := v.e.UpdateRowsWhere(ctx, req.GetTable(), match, set, token)
	if err != nil {
		return nil, v.e.rpcError(ctx, "update where", err)
	}

	meta := &cachetv2.WriteMeta{Version: uint64(out.Newest)}
	if out.Degraded {
		meta.Degraded = true
		meta.DegradedReason = degradedReason
		meta.EffectiveStalenessBound = durationpb.New(v.e.cdcLagBound)
	}
	return &cachetv2.UpdateWhereResponse{
		Matched:      out.Matched,
		AffectedKeys: out.AffectedKeys,
		Meta:         meta,
		Session:      sessionToV2(token.Proto()),
	}, nil
}

// key parses a request's key and checks this engine serves the table it names.
func (v *V2) key(raw string) (schema.Key, error) {
	key, _, err := v.e.parseKey(raw)
	if err != nil {
		return schema.Key{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return key, nil
}

func v2ReadMeta(level consistency.Level, res RowResult) *cachetv2.ReadMeta {
	return &cachetv2.ReadMeta{
		LevelServed: cachetv2.ConsistencyLevel(level.Proto()),
		CacheHit:    res.CacheHit,
		RowVersion:  uint64(res.RowVersion),
		FillVersion: uint64(res.FillVersion),
	}
}

func rowToProto(row storage.Row) *cachetv2.Row {
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

func rowFromProto(row *cachetv2.Row) (storage.Row, error) {
	if row == nil || len(row.GetValues()) == 0 {
		return nil, fmt.Errorf("engine: put requires a row")
	}
	out := make(storage.Row, len(row.GetValues()))
	for i, v := range row.GetValues() {
		if v.GetIsNull() {
			out[i] = schema.Null()
			continue
		}
		out[i] = schema.Bin(v.GetData())
	}
	return out, nil
}

func columnValues(cmp []*cachetv2.Comparison) ([]storage.ColumnValue, error) {
	out := make([]storage.ColumnValue, 0, len(cmp))
	for _, c := range cmp {
		if c.GetColumn() == "" {
			return nil, fmt.Errorf("engine: a comparison names no column")
		}
		out = append(out, storage.ColumnValue{Column: c.GetColumn(), Value: valueFromProto(c.GetValue())})
	}
	return out, nil
}

func assignments(as []*cachetv2.Assignment) ([]storage.ColumnValue, error) {
	out := make([]storage.ColumnValue, 0, len(as))
	for _, a := range as {
		if a.GetColumn() == "" {
			return nil, fmt.Errorf("engine: an assignment names no column")
		}
		out = append(out, storage.ColumnValue{Column: a.GetColumn(), Value: valueFromProto(a.GetValue())})
	}
	return out, nil
}

func valueFromProto(v *cachetv2.Value) schema.Value {
	if v.GetIsNull() {
		return schema.Null()
	}
	return schema.Bin(v.GetData())
}

func tablesToProto(tables []*schema.Descriptor) []*cachetv2.TableDescriptor {
	out := make([]*cachetv2.TableDescriptor, 0, len(tables))
	for _, d := range tables {
		out = append(out, descriptorToProto(d))
	}
	return out
}

func descriptorToProto(d *schema.Descriptor) *cachetv2.TableDescriptor {
	out := &cachetv2.TableDescriptor{
		Name:          d.Name,
		VersionColumn: d.VersionColumn.Name,
		Fingerprint:   d.Fingerprint,
	}
	for i := range d.Columns {
		out.Columns = append(out.Columns, &cachetv2.ColumnDescriptor{
			Name:     d.Columns[i].Name,
			Type:     string(d.Columns[i].Type),
			Nullable: d.Columns[i].Nullable,
		})
	}
	for _, c := range d.PrimaryKey {
		out.PrimaryKey = append(out.PrimaryKey, c.Name)
	}
	return out
}
