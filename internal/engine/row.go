package engine

import (
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// The bridge between the typed fixture record and the generic row encoding.
//
// The cache layer, the wire protocol and the proxy are all schema-agnostic: they carry an encoded
// row and a fingerprint and never look inside either. Storage's generic path exists but the
// engine's read and write paths still call the typed one, so this file translates between the two.
//
// It exists to be deleted. When the engine reads rows against a declared descriptor, these
// conversions become the identity and this file goes with them. Until then it is what bounds what
// a declaration may say: the engine accepts a table with any NAME, but only the column shape the
// typed path can carry, and it says exactly that when it refuses.

// legacyShape is the row shape the typed storage path carries.
//
// The name is deliberately not part of it. A deployment declaring a table called `widgets` with
// these five columns is served correctly today; one declaring different columns is refused at boot
// rather than half-served.
var legacyShape = func() *schema.Descriptor {
	d, err := schema.NewDescriptor(legacyShapeConfig("entities"))
	if err != nil {
		// Unreachable: the declaration is a constant in this file. A panic here means the
		// descriptor rules changed under a shape that ships with Cachet.
		panic("engine: the built-in row shape no longer validates: " + err.Error())
	}
	return d
}()

func legacyShapeConfig(name string) schema.TableConfig {
	return schema.TableConfig{
		Name:          name,
		PrimaryKey:    []string{"id"},
		VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: schema.Uint64},
			{Name: "tenant_id", Type: schema.Uint32},
			{Name: "status", Type: schema.Uint8},
			{Name: "payload", Type: schema.Bytes},
			{Name: "version", Type: schema.Uint64},
		},
	}
}

// declaredTables validates what an engine has been asked to serve.
func declaredTables(declared []*schema.Descriptor) (map[string]*schema.Descriptor, []string, error) {
	if len(declared) == 0 {
		return nil, nil, fmt.Errorf("engine: no table declared; Cachet has no built-in table")
	}

	// One table, for now. The cache, the wire protocol and the proxy all carry rows generically;
	// the engine's own read and write paths do not yet, and serving a second table would mean
	// routing to a storage call that still names the first one's columns. Refusing here is the
	// difference between a boot failure an operator can read and rows quietly going to the wrong
	// place.
	if len(declared) > 1 {
		names := make([]string, 0, len(declared))
		for _, d := range declared {
			names = append(names, d.Name)
		}
		return nil, nil, fmt.Errorf(
			"engine: %d tables are declared (%v), and this build serves one", len(declared), names)
	}

	tables := make(map[string]*schema.Descriptor, len(declared))
	order := make([]string, 0, len(declared))
	for _, d := range declared {
		if err := checkShape(d); err != nil {
			return nil, nil, err
		}
		if _, dup := tables[d.Name]; dup {
			return nil, nil, fmt.Errorf("engine: table %q is declared twice", d.Name)
		}
		tables[d.Name] = d
		order = append(order, d.Name)
	}
	return tables, order, nil
}

// checkShape refuses a declaration the typed storage path cannot carry.
//
// Column by column rather than by comparing fingerprints, because a fingerprint mismatch says only
// that something differs. An operator reading this error needs to know which column and how.
func checkShape(d *schema.Descriptor) error {
	want := legacyShape
	if len(d.Columns) != len(want.Columns) {
		return fmt.Errorf("engine: table %q declares %d columns; this build's storage path carries %d (%s)",
			d.Name, len(d.Columns), len(want.Columns), columnList(want))
	}
	for i := range want.Columns {
		w, g := &want.Columns[i], &d.Columns[i]
		if w.Name != g.Name || w.Type != g.Type || w.Nullable != g.Nullable {
			return fmt.Errorf("engine: table %q declares column %d as %s %s (nullable=%t); "+
				"this build's storage path carries %s %s (nullable=%t)",
				d.Name, i, g.Name, g.Type, g.Nullable, w.Name, w.Type, w.Nullable)
		}
	}
	if len(d.PrimaryKey) != 1 || d.PrimaryKey[0].Name != want.PrimaryKey[0].Name {
		return fmt.Errorf("engine: table %q must declare %q as its single primary key column",
			d.Name, want.PrimaryKey[0].Name)
	}
	if d.VersionColumn.Name != want.VersionColumn.Name {
		return fmt.Errorf("engine: table %q declares %q as its version column; this build's storage path uses %q",
			d.Name, d.VersionColumn.Name, want.VersionColumn.Name)
	}
	return nil
}

func columnList(d *schema.Descriptor) string {
	out := ""
	for i := range d.Columns {
		if i > 0 {
			out += ", "
		}
		out += d.Columns[i].Name + " " + string(d.Columns[i].Type)
	}
	return out
}

// Table returns the declared shape of a table this engine serves.
//
// The proxy builds its patterns and its statements from the same declaration the engine caches
// against. Two declarations for one table would be two things to keep in step, and the failure
// mode of their drifting is the proxy answering a column list the engine's entries cannot fill.
func (e *Engine) Table(name string) (*schema.Descriptor, error) {
	d, ok := e.tables[name]
	if !ok {
		return nil, fmt.Errorf("engine: no table named %q is served by this engine", name)
	}
	return d, nil
}

// Tables returns every declared table, in declaration order.
func (e *Engine) Tables() []*schema.Descriptor {
	out := make([]*schema.Descriptor, 0, len(e.tableOrder))
	for _, name := range e.tableOrder {
		out = append(out, e.tables[name])
	}
	return out
}

// table is the single declared table, which most of the engine still assumes.
func (e *Engine) table() *schema.Descriptor { return e.tables[e.tableOrder[0]] }

// Fingerprint is the shape of this build's own fixture table.
//
// It is NOT what a deployment should configure its cache client with: the fingerprint is derived
// from the declaration, table name included, so a deployment caching `widgets` has its own. Use
// the declared descriptor's Fingerprint. This exists for the project's own fixtures, and for the
// assertion that declaring the fixture table in config reproduces the value it always had.
func Fingerprint() string { return legacyShape.Fingerprint }

// encodeRecord renders a storage record as an encoded row.
func encodeRecord(rec storage.Record) ([]byte, error) {
	return legacyShape.EncodeRow([]schema.Value{
		schema.Uint(rec.ID),
		schema.Uint(uint64(rec.TenantID)),
		schema.Uint(uint64(rec.Status)),
		schema.Bin(rec.Payload),
		schema.Uint(uint64(rec.Version)),
	})
}

// decodeRecord recovers a storage record from an encoded row.
func decodeRecord(row []byte) (storage.Record, error) {
	values, err := legacyShape.DecodeRow(row)
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
		return storage.Record{}, fmt.Errorf("engine: encoded row does not fit the record shape")
	}
	return storage.Record{
		ID:       id,
		TenantID: uint32(tenant),
		Status:   uint8(status),
		Payload:  values[3].Bytes,
		Version:  storage.Version(version),
	}, nil
}
