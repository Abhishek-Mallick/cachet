// Package table declares the project's own fixture table.
//
// Cachet has no built-in table: what it caches is declared in config (ADR 0005). Its own fixtures
// are no exception, so the declaration that matches test/fixtures/schema/entities.sql lives here —
// once — rather than being retyped in every package that needs a config to validate.
//
// It is next to the SQL deliberately. The schema and the declaration have to agree, and the way
// they stay in agreement is by being neighbours that a single change touches together.
package table

import (
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// Name is the fixture table.
const Name = "entities"

// Entities is the declaration matching test/fixtures/schema/entities.sql.
//
// The column ORDER is part of the row encoding and of the fingerprint, so it is not cosmetic:
// reordering it makes every existing cache entry read as a miss. `updated_at` is deliberately
// absent — it exists in the schema and not in a cache entry, which is what makes `SELECT *`
// unanswerable from the cache and gives the proxy's whole-row proof something to refuse.
func Entities() config.Table {
	return config.Table{
		Topology: "main",
		TableConfig: schema.TableConfig{
			Name:          Name,
			PrimaryKey:    []string{"id"},
			VersionColumn: "version",
			Columns: []schema.ColumnConfig{
				{Name: "id", Type: schema.Uint64},
				{Name: "tenant_id", Type: schema.Uint32},
				{Name: "status", Type: schema.Uint8},
				{Name: "payload", Type: schema.Bytes},
				{Name: "version", Type: schema.Uint64},
			},
		},
		Predicates: []config.Predicate{{Match: []string{"tenant_id", "status"}, Set: []string{"status"}}},
	}
}

// Descriptor is the fixture table's validated shape.
func Descriptor() *schema.Descriptor {
	d, err := Entities().Descriptor()
	if err != nil {
		panic("fixtures: the fixture table declaration no longer validates: " + err.Error())
	}
	return d
}

// Declare puts the fixture table on a config, with one topology holding every shard it declares.
func Declare(cfg config.Config) config.Config {
	ids := make([]string, 0, len(cfg.Shards))
	for _, sh := range cfg.Shards {
		ids = append(ids, sh.ID)
	}
	cfg.Topologies = []config.Topology{{Name: "main", Shards: ids}}
	cfg.Tables = []config.Table{Entities()}
	return cfg
}

// Row builds a fixture row in the descriptor's column order.
//
// The version is a placeholder: storage stamps its own from the shard's HLC, which is the only
// clock allowed to issue one.
func Row(id uint64, tenant uint32, status uint8, payload string) storage.Row {
	return storage.Row{
		schema.Uint(id),
		schema.Uint(uint64(tenant)),
		schema.Uint(uint64(status)),
		schema.Bin([]byte(payload)),
		schema.Uint(0),
	}
}

// Key names a fixture row.
func Key(id uint64) schema.Key {
	k, err := schema.KeyOf(Name, id)
	if err != nil {
		panic("fixtures: the fixture key grammar no longer accepts an integer id: " + err.Error())
	}
	return k
}

// VersionOf reads a fixture row's version column.
func VersionOf(row storage.Row) (storage.Version, error) {
	v, err := row[4].Uint64()
	if err != nil {
		return 0, err
	}
	return storage.Version(v), nil
}

// ─── the second table ───────────────────────────────────────────────────────────

// GadgetsName is the second fixture table.
const GadgetsName = "gadgets"

// Gadgets is the declaration matching the `gadgets` table in
// test/fixtures/schema/entities.sql.
//
// Deliberately unlike the first one in every way that used to be compiled in: a STRING primary
// key, a nullable column, a DECIMAL carried as text, and a version column that is not called
// `version`. It is what makes "Cachet caches an arbitrary table" a claim the suite executes.
func Gadgets() config.Table {
	return config.Table{
		Topology: "main",
		TableConfig: schema.TableConfig{
			Name:          GadgetsName,
			PrimaryKey:    []string{"sku"},
			VersionColumn: "row_version",
			Columns: []schema.ColumnConfig{
				{Name: "sku", Type: schema.String, Collation: "utf8mb4_bin"},
				{Name: "region", Type: schema.String, Collation: "utf8mb4_bin"},
				{Name: "price", Type: schema.Text},
				{Name: "note", Type: schema.Text, Nullable: true},
				{Name: "row_version", Type: schema.Uint64},
			},
		},
		Predicates: []config.Predicate{{Match: []string{"region"}, Set: []string{"note"}}},
	}
}

// GadgetsDescriptor is the second fixture table's validated shape.
func GadgetsDescriptor() *schema.Descriptor {
	d, err := Gadgets().Descriptor()
	if err != nil {
		panic("fixtures: the gadgets declaration no longer validates: " + err.Error())
	}
	return d
}

// GadgetRow builds a gadgets row. row_version is a placeholder; storage stamps its own.
func GadgetRow(sku, region, price string, note *string) storage.Row {
	noteValue := schema.Null()
	if note != nil {
		noteValue = schema.Str(*note)
	}
	return storage.Row{
		schema.Str(sku),
		schema.Str(region),
		schema.Str(price),
		noteValue,
		schema.Uint(0),
	}
}

// GadgetKey names a gadgets row.
func GadgetKey(sku string) schema.Key {
	k, err := schema.KeyOf(GadgetsName, sku)
	if err != nil {
		panic("fixtures: the gadgets key grammar rejected a sku: " + err.Error())
	}
	return k
}

// DeclareBoth puts both fixture tables on a config, on one topology holding every shard.
func DeclareBoth(cfg config.Config) config.Config {
	cfg = Declare(cfg)
	cfg.Tables = append(cfg.Tables, Gadgets())
	return cfg
}
