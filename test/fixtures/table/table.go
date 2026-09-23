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
