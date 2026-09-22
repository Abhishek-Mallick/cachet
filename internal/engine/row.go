package engine

import (
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// The bridge between the typed fixture record and the generic row encoding.
//
// The cache layer is now schema-agnostic: it carries an encoded row and a fingerprint and never
// looks inside either. Storage is not generic yet, so this file translates between the two — one
// descriptor, matching the fixture table, built once.
//
// It exists to be deleted. When storage produces rows against a configured descriptor, these
// conversions become the identity and this file goes with them.

var entitiesDescriptor = func() *schema.Descriptor {
	d, err := schema.NewDescriptor(schema.TableConfig{
		Name:          "entities",
		PrimaryKey:    []string{"id"},
		VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: schema.Uint64},
			{Name: "tenant_id", Type: schema.Uint32},
			{Name: "status", Type: schema.Uint8},
			{Name: "payload", Type: schema.Bytes},
			{Name: "version", Type: schema.Uint64},
		},
	})
	if err != nil {
		// Unreachable: the declaration is a constant in this file. A panic here means the
		// descriptor rules changed under a table that ships with Cachet.
		panic("engine: the built-in entities descriptor no longer validates: " + err.Error())
	}
	return d
}()

// Fingerprint is the row shape this build reads and writes.
func Fingerprint() string { return entitiesDescriptor.Fingerprint }

// encodeRecord renders a storage record as an encoded row.
func encodeRecord(rec storage.Record) ([]byte, error) {
	return entitiesDescriptor.EncodeRow([]schema.Value{
		schema.Uint(rec.ID),
		schema.Uint(uint64(rec.TenantID)),
		schema.Uint(uint64(rec.Status)),
		schema.Bin(rec.Payload),
		schema.Uint(uint64(rec.Version)),
	})
}

// decodeRecord recovers a storage record from an encoded row.
func decodeRecord(row []byte) (storage.Record, error) {
	values, err := entitiesDescriptor.DecodeRow(row)
	if err != nil {
		return storage.Record{}, err
	}
	if len(values) != 5 {
		return storage.Record{}, fmt.Errorf("engine: encoded row has %d columns, want 5", len(values))
	}

	id, err := values[0].Uint64()
	if err != nil {
		return storage.Record{}, err
	}
	tenant, err := values[1].Uint64()
	if err != nil {
		return storage.Record{}, err
	}
	status, err := values[2].Uint64()
	if err != nil {
		return storage.Record{}, err
	}
	version, err := values[4].Uint64()
	if err != nil {
		return storage.Record{}, err
	}
	if tenant > 0xffff_ffff || status > 0xff {
		return storage.Record{}, fmt.Errorf("engine: encoded row does not fit the fixture record shape")
	}

	return storage.Record{
		ID:       id,
		TenantID: uint32(tenant),
		Status:   uint8(status),
		Payload:  values[3].Bytes,
		Version:  storage.Version(version),
	}, nil
}
