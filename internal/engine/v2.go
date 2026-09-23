package engine

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// ProtocolVersionV2 is the generic-row wire contract.
//
// Served alongside cachet.v1 for one release. v1 encodes the fixture table's schema into the
// protocol itself, so it cannot describe anybody else's table; it stays until 1.0 so that a client
// built against v0.1.0 keeps working across the upgrade rather than discovering the change as a
// connection error.
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
		Tables:          []*cachetv2.TableDescriptor{descriptorToProto(entitiesDescriptor)},
	}, nil
}

// Get reads one row.
func (v *V2) Get(ctx context.Context, req *cachetv2.GetRequest) (*cachetv2.GetResponse, error) {
	v1req, err := getRequestToV1(req)
	if err != nil {
		return nil, err
	}
	v1resp, err := v.e.Get(ctx, v1req)
	if err != nil {
		return nil, err
	}

	out := &cachetv2.GetResponse{
		Found:   v1resp.GetFound(),
		Meta:    readMetaToV2(v1resp.GetMeta()),
		Session: sessionToV2(v1resp.GetSession()),
	}
	if v1resp.GetFound() {
		out.Row = recordToRow(v1resp.GetRecord())
	}
	return out, nil
}

// Put writes one row.
func (v *V2) Put(ctx context.Context, req *cachetv2.PutRequest) (*cachetv2.PutResponse, error) {
	key, err := schema.ParseKey(req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rec, err := rowToRecord(key, req.GetRow())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	v1resp, err := v.e.Put(ctx, putRequestToV1(req, rec))
	if err != nil {
		return nil, err
	}
	return &cachetv2.PutResponse{
		Meta:    writeMetaToV2(v1resp.GetMeta()),
		Session: sessionToV2(v1resp.GetSession()),
	}, nil
}

// Delete removes one row.
func (v *V2) Delete(ctx context.Context, req *cachetv2.DeleteRequest) (*cachetv2.DeleteResponse, error) {
	v1resp, err := v.e.Delete(ctx, deleteRequestToV1(req))
	if err != nil {
		return nil, err
	}
	return &cachetv2.DeleteResponse{
		Existed: v1resp.GetExisted(),
		Meta:    writeMetaToV2(v1resp.GetMeta()),
		Session: sessionToV2(v1resp.GetSession()),
	}, nil
}

// BatchGet reads several rows.
func (v *V2) BatchGet(ctx context.Context, req *cachetv2.BatchGetRequest) (*cachetv2.BatchGetResponse, error) {
	v1resp, err := v.e.BatchGet(ctx, batchGetRequestToV1(req))
	if err != nil {
		return nil, err
	}

	rows := make(map[string]*cachetv2.Row, len(v1resp.GetRecords()))
	for key, rec := range v1resp.GetRecords() {
		rows[key] = recordToRow(rec)
	}
	return &cachetv2.BatchGetResponse{
		Rows:    rows,
		Meta:    readMetaToV2(v1resp.GetMeta()),
		Session: sessionToV2(v1resp.GetSession()),
	}, nil
}

// UpdateWhere applies a conditional write.
func (v *V2) UpdateWhere(ctx context.Context, req *cachetv2.UpdateWhereRequest) (*cachetv2.UpdateWhereResponse, error) {
	v1req, err := updateWhereToV1(req)
	if err != nil {
		return nil, err
	}
	v1resp, err := v.e.UpdateWhere(ctx, v1req)
	if err != nil {
		return nil, err
	}
	return &cachetv2.UpdateWhereResponse{
		Meta:         writeMetaToV2(v1resp.GetMeta()),
		Matched:      v1resp.GetMatched(),
		AffectedKeys: v1resp.GetAffectedKeys(),
		Session:      sessionToV2(v1resp.GetSession()),
	}, nil
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
