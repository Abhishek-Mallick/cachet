package engine

import (
	"fmt"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// Translation between the two wire contracts.
//
// It exists in one file so that the places where v1's fixed shape meets v2's generic one are
// countable. Every one of them is a place the two protocols could come to disagree, and the point
// of serving both from a single engine is that they cannot.
//
// The direction of travel is v2 → v1 → engine for now, because the engine still speaks the typed
// record. When storage's generic path reaches the engine, this file inverts and v1 becomes the
// adapter — at which point the shim below is the only thing left translating.

// rowToRecord reads a v2 row as the fixture record.
//
// This is the v1 SHIM, and the reason it can be this strict: v1 can only describe a table shaped
// like the fixture, so a v2 row that is not that shape has no v1 representation. Refusing with the
// reason named beats inventing columns, which would be a wrong answer dressed as a working one.
func rowToRecord(d *schema.Descriptor, key schema.Key, row *cachetv2.Row) (*cachetv1.Record, error) {
	values := row.GetValues()
	if len(values) != len(d.Columns) {
		return nil, fmt.Errorf("row has %d values, this engine's table has %d columns",
			len(values), len(d.Columns))
	}
	if len(key.Values) != 1 {
		return nil, fmt.Errorf("key %s is composite; this engine's table has a single-column primary key", key)
	}

	decoded := make([]schema.Value, len(values))
	for i, v := range values {
		if v.GetIsNull() {
			decoded[i] = schema.Null()
			continue
		}
		decoded[i] = schema.Bin(v.GetData())
	}

	// Encoding and decoding rather than reading positions directly: it runs the row through the
	// same validation a cached row gets, so a NULL in a NOT NULL column is refused here rather
	// than written to the database and refused on the way back out.
	encoded, err := d.EncodeRow(decoded)
	if err != nil {
		return nil, err
	}
	rec, err := decodeRecord(encoded)
	if err != nil {
		return nil, err
	}

	// The key and the row's primary-key column have to agree. They arrive as separate fields, so a
	// client that built one from a stale copy of the other would have the engine write a record
	// under a key that does not name it — a row findable only by a key nobody would construct.
	want, err := d.Key(rec.ID)
	if err != nil {
		return nil, err
	}
	if want.String() != key.String() {
		return nil, fmt.Errorf("key %s does not name the row it carries, which is %s", key, want)
	}
	return recordToProto(rec), nil
}

// recordToRow renders the fixture record as a v2 row.
func recordToRow(rec *cachetv1.Record) *cachetv2.Row {
	vals := []any{
		rec.GetId(), uint64(rec.GetTenantId()), uint64(rec.GetStatus()), rec.GetPayload(), rec.GetVersion(),
	}
	out := &cachetv2.Row{Values: make([]*cachetv2.Value, 0, len(vals))}
	for _, v := range vals {
		switch t := v.(type) {
		case uint64:
			out.Values = append(out.Values, &cachetv2.Value{Data: schema.Uint(t).Bytes})
		case []byte:
			out.Values = append(out.Values, &cachetv2.Value{Data: t})
		}
	}
	return out
}

func getRequestToV1(req *cachetv2.GetRequest) (*cachetv1.GetRequest, error) {
	if _, err := schema.ParseKey(req.GetKey()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &cachetv1.GetRequest{
		Key:            req.GetKey(),
		Level:          cachetv1.ConsistencyLevel(req.GetLevel()),
		StalenessBound: req.GetStalenessBound(),
		Session:        sessionToV1(req.GetSession()),
	}, nil
}

func batchGetRequestToV1(req *cachetv2.BatchGetRequest) *cachetv1.BatchGetRequest {
	return &cachetv1.BatchGetRequest{
		Keys:           req.GetKeys(),
		Level:          cachetv1.ConsistencyLevel(req.GetLevel()),
		StalenessBound: req.GetStalenessBound(),
		Session:        sessionToV1(req.GetSession()),
	}
}

func putRequestToV1(req *cachetv2.PutRequest, rec *cachetv1.Record) *cachetv1.PutRequest {
	return &cachetv1.PutRequest{
		Key:     req.GetKey(),
		Record:  rec,
		Session: sessionToV1(req.GetSession()),
	}
}

func deleteRequestToV1(req *cachetv2.DeleteRequest) *cachetv1.DeleteRequest {
	return &cachetv1.DeleteRequest{Key: req.GetKey(), Session: sessionToV1(req.GetSession())}
}

// updateWhereToV1 narrows a generic predicate onto the one shape v1 can express.
//
// v1's predicate is tenant_id AND status → status, which is the fixture's conditional write and
// nothing else. A predicate naming other columns is refused with the columns listed, because
// silently matching on the nearest available column would change which rows a write touched.
func updateWhereToV1(d *schema.Descriptor, req *cachetv2.UpdateWhereRequest) (*cachetv1.UpdateWhereRequest, error) {
	// v1's conditional write has no table field because there was only ever one table. v2 names
	// it, so the name has to be checked here or it would be accepted and ignored — a write the
	// caller believes landed on one table and that landed on another.
	if t := req.GetTable(); t != "" && t != d.Name {
		return nil, status.Errorf(codes.InvalidArgument,
			"this engine serves table %q; it cannot write to %q", d.Name, t)
	}

	out := &cachetv1.UpdateWhereRequest{Session: sessionToV1(req.GetSession())}

	var sawTenant, sawStatus bool
	for _, m := range req.GetMatch() {
		v, err := uint32Value(m.GetValue())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "match on %s: %v", m.GetColumn(), err)
		}
		switch m.GetColumn() {
		case "tenant_id":
			out.TenantId = v
			sawTenant = true
		case "status":
			out.MatchStatus = v
			sawStatus = true
		default:
			return nil, status.Errorf(codes.InvalidArgument,
				"this engine's conditional write matches on tenant_id and status; it cannot match on %q",
				m.GetColumn())
		}
	}
	if !sawTenant || !sawStatus {
		return nil, status.Error(codes.InvalidArgument,
			"this engine's conditional write requires matching on both tenant_id and status")
	}

	if len(req.GetSet()) != 1 || req.GetSet()[0].GetColumn() != "status" {
		return nil, status.Error(codes.InvalidArgument,
			"this engine's conditional write sets status and nothing else")
	}
	v, err := uint32Value(req.GetSet()[0].GetValue())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "set status: %v", err)
	}
	out.SetStatus = v
	return out, nil
}

// uint32Value narrows here rather than at the call sites: every column v1's predicate names is a
// uint32 on the wire, so the width check belongs with the parse and the conversion cannot then be
// written anywhere that has not already been bounded.
func uint32Value(v *cachetv2.Value) (uint32, error) {
	if v.GetIsNull() {
		return 0, fmt.Errorf("value is NULL")
	}
	u, err := schema.Bin(v.GetData()).Uint64()
	if err != nil {
		return 0, err
	}
	if u > math.MaxUint32 {
		return 0, fmt.Errorf("%d does not fit the column", u)
	}
	return uint32(u), nil
}

func sessionToV1(s *cachetv2.SessionToken) *cachetv1.SessionToken {
	if s == nil {
		return nil
	}
	return &cachetv1.SessionToken{Watermarks: s.GetWatermarks()}
}

func sessionToV2(s *cachetv1.SessionToken) *cachetv2.SessionToken {
	if s == nil {
		return nil
	}
	return &cachetv2.SessionToken{Watermarks: s.GetWatermarks()}
}

func readMetaToV2(m *cachetv1.ReadMeta) *cachetv2.ReadMeta {
	if m == nil {
		return nil
	}
	return &cachetv2.ReadMeta{
		LevelServed:    cachetv2.ConsistencyLevel(m.GetLevelServed()),
		Degraded:       m.GetDegraded(),
		DegradedReason: m.GetDegradedReason(),
		CacheHit:       m.GetCacheHit(),
		RowVersion:     m.GetRowVersion(),
		FillVersion:    m.GetFillVersion(),
	}
}

func writeMetaToV2(m *cachetv1.WriteMeta) *cachetv2.WriteMeta {
	if m == nil {
		return nil
	}
	return &cachetv2.WriteMeta{
		Version:                 m.GetVersion(),
		Degraded:                m.GetDegraded(),
		DegradedReason:          m.GetDegradedReason(),
		EffectiveStalenessBound: m.GetEffectiveStalenessBound(),
	}
}
