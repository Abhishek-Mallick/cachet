package config

import (
	"errors"
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// The declared tables, and the shard sets they live on.
//
// Until this existed, the cached table's shape was compiled into Cachet: five columns, a primary
// key called `id`, a version column called `version`. Everything above the storage layer was real
// and all of it operated on a fixture. A team could not cache their own schema, which outranked
// every other item on the roadmap.
//
// Declared rather than discovered. Cachet will not read INFORMATION_SCHEMA and decide for itself
// what to cache: the cacheable set is an operator's decision with consistency consequences, and a
// system that chose it would be choosing which rows get a weaker guarantee. Introspection is used
// to CHECK the declaration at boot, never to write it (ADR 0005).

// Topology is a named set of database shards.
//
// Tables name a topology rather than listing shards directly, because routing is derived from the
// key and the key is namespaced by table: `orders:5` and `users:5` hash to different positions on
// the ring, and that is only correct if both tables are sharded the same way. Two tables that must
// be read together belong to one topology; a table sharded differently gets its own.
type Topology struct {
	Name   string   `koanf:"name"`
	Shards []string `koanf:"shards"`
}

// Predicate is a conditional-write shape the deployment permits.
//
// Declared rather than taken from a request. Identifiers never arrive over the wire — anything
// richer re-creates the query-proxy problem this architecture exists to escape — and a predicate's
// match columns are validated against the table's indexes at boot, because `SELECT … FOR UPDATE`
// on an unindexed column takes gap locks inside the transaction that is about to write. That is an
// outage rather than a slow query, and it only appears under the concurrency production has.
type Predicate struct {
	Match []string `koanf:"match"`
	Set   []string `koanf:"set"`
}

// Table is one declared table.
type Table struct {
	schema.TableConfig `koanf:",squash"`

	// Topology names the shard set this table lives on.
	Topology string `koanf:"topology"`

	Predicates []Predicate `koanf:"predicates"`
}

// Descriptor validates the declaration and returns the table's shape.
func (t Table) Descriptor() (*schema.Descriptor, error) {
	return schema.NewDescriptor(t.TableConfig)
}

// PredicateSpecs renders the declared predicates for the storage layer.
func (t Table) PredicateSpecs() []storage.PredicateSpec {
	out := make([]storage.PredicateSpec, 0, len(t.Predicates))
	for _, p := range t.Predicates {
		out = append(out, storage.PredicateSpec{Match: p.Match, Set: p.Set})
	}
	return out
}

// Descriptors builds every declared table's descriptor, in declaration order.
//
// Order is preserved because it is what the handshake publishes and what an operator reads in a
// log line; a map would make both arbitrary between runs.
func (c Config) Descriptors() ([]*schema.Descriptor, error) {
	out := make([]*schema.Descriptor, 0, len(c.Tables))
	for i, t := range c.Tables {
		d, err := t.Descriptor()
		if err != nil {
			return nil, fmt.Errorf("config: tables[%d]: %w", i, err)
		}
		out = append(out, d)
	}
	return out, nil
}

// TableNamed returns one declared table.
func (c Config) TableNamed(name string) (Table, bool) {
	for _, t := range c.Tables {
		if t.Name == name {
			return t, true
		}
	}
	return Table{}, false
}

// ShardsFor returns the shards a table's topology names, in declared order.
func (c Config) ShardsFor(table string) ([]Shard, error) {
	t, ok := c.TableNamed(table)
	if !ok {
		return nil, fmt.Errorf("config: no table named %q is declared", table)
	}
	for _, topo := range c.Topologies {
		if topo.Name != t.Topology {
			continue
		}
		out := make([]Shard, 0, len(topo.Shards))
		for _, id := range topo.Shards {
			for _, sh := range c.Shards {
				if sh.ID == id {
					out = append(out, sh)
					break
				}
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("config: table %q names topology %q, which is not declared", table, t.Topology)
}

// validateTables checks the declarations against each other and against the shard list.
//
// Everything here is a check that can be made without a database. The ones that need one — does
// the table exist, do its columns match the declaration, is every predicate's match set covered by
// an index — happen at boot against INFORMATION_SCHEMA, because a config file cannot know them and
// pretending otherwise would move the failure from startup to the first request.
func (c Config) validateTables() error {
	if len(c.Topologies) == 0 {
		return errNoTopologies
	}
	if len(c.Tables) == 0 {
		return errNoTables
	}

	declaredShards := make(map[string]struct{}, len(c.Shards))
	for _, sh := range c.Shards {
		declaredShards[sh.ID] = struct{}{}
	}

	topologies := make(map[string]struct{}, len(c.Topologies))
	for i, topo := range c.Topologies {
		if topo.Name == "" {
			return fmt.Errorf("config: topologies[%d].name is empty", i)
		}
		if _, dup := topologies[topo.Name]; dup {
			return fmt.Errorf("config: duplicate topology name %q", topo.Name)
		}
		topologies[topo.Name] = struct{}{}

		if len(topo.Shards) == 0 {
			return fmt.Errorf("config: topology %q names no shards", topo.Name)
		}
		seen := make(map[string]struct{}, len(topo.Shards))
		for _, id := range topo.Shards {
			if _, ok := declaredShards[id]; !ok {
				return fmt.Errorf("config: topology %q names shard %q, which no entry under shards: declares", topo.Name, id)
			}
			if _, dup := seen[id]; dup {
				// A repeated shard would take two positions on the ring and receive twice its
				// share of the key space, which is a capacity plan that is quietly wrong.
				return fmt.Errorf("config: topology %q names shard %q twice", topo.Name, id)
			}
			seen[id] = struct{}{}
		}
	}

	tables := make(map[string]struct{}, len(c.Tables))
	for i, t := range c.Tables {
		d, err := t.Descriptor()
		if err != nil {
			return fmt.Errorf("config: tables[%d]: %w", i, err)
		}
		if _, dup := tables[d.Name]; dup {
			// Two declarations of one table are two shapes for one set of cache entries, and the
			// entries carry a fingerprint of whichever shape wrote them.
			return fmt.Errorf("config: duplicate table %q", d.Name)
		}
		tables[d.Name] = struct{}{}

		if t.Topology == "" {
			return fmt.Errorf("config: table %q names no topology", d.Name)
		}
		if _, ok := topologies[t.Topology]; !ok {
			return fmt.Errorf("config: table %q names topology %q, which is not declared", d.Name, t.Topology)
		}

		if err := validatePredicates(d, t.Predicates); err != nil {
			return err
		}
	}
	return nil
}

// validatePredicates checks that a predicate names columns the table actually has.
//
// The rest of the rules — never setting the version column, never moving a row's primary key, and
// index coverage — belong to storage.NewTable, which is where they can be checked against the live
// table. Duplicating them here would be two places to keep in step.
func validatePredicates(d *schema.Descriptor, predicates []Predicate) error {
	for j, p := range predicates {
		if len(p.Match) == 0 {
			return fmt.Errorf("config: table %q predicates[%d] matches on no columns", d.Name, j)
		}
		if len(p.Set) == 0 {
			return fmt.Errorf("config: table %q predicates[%d] sets no columns", d.Name, j)
		}
		for _, name := range append(append([]string{}, p.Match...), p.Set...) {
			if d.Column(name) == nil {
				return fmt.Errorf("config: table %q predicates[%d] names column %q, which is not declared",
					d.Name, j, name)
			}
		}
	}
	return nil
}

// The two absences that need a longer explanation than "field missing".
//
// They are the one breaking change in this release for a deployment that already had a working
// config, so the error says what to run rather than only what is wrong.
var (
	errNoTopologies = errors.New(
		"config: topologies: must declare at least one shard set. " +
			"Run `cachetctl config migrate -config <file>` to generate one from your shards")
	errNoTables = errors.New(
		"config: tables: must declare at least one table. Cachet has no built-in table — " +
			"run `cachetctl config migrate -config <file>` to write the declaration Cachet used to compile in")
)
