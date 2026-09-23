package engine

import (
	"errors"
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// What an engine serves, and where each table lives.
//
// Cachet has no built-in table: the declaration arrives from config (ADR 0005). Routing is derived
// from the key and the key is namespaced by table, so `orders:5` and `users:5` land in different
// places on the ring — correct only if each table is routed over the shards it was declared on.
// A single ring shared by every table would be correct only by coincidence.

// ErrInvalidRequest marks a failure the CALLER caused rather than the system.
//
// It is what separates "this engine does not serve that table" from "the database is down". Both
// fail a request; only one is worth waking someone for, and only one the caller can act on.
var ErrInvalidRequest = errors.New("engine: invalid request")

// TableSpec is one declared table and the shards it lives on.
type TableSpec struct {
	Descriptor *schema.Descriptor

	// Shards is the table's topology, in declared order. Order matters: the ring is built from it,
	// and reordering would move keys.
	Shards []storage.ShardID
}

// table is a declared table with its routing resolved.
type table struct {
	d      *schema.Descriptor
	router *storage.Router
	shards map[storage.ShardID]*storage.Shard
}

// shardFor routes a key to the shard holding it.
func (t *table) shardFor(key string) (*storage.Shard, storage.ShardID, error) {
	id, err := t.router.ShardFor(key)
	if err != nil {
		return nil, "", fmt.Errorf("engine: route %s: %w", key, err)
	}
	s, ok := t.shards[id]
	if !ok {
		return nil, "", fmt.Errorf("engine: shard %q holds %s but has no connection", id, key)
	}
	return s, id, nil
}

// buildTables resolves each declared table onto the shard connections it was given.
func buildTables(specs []TableSpec, shards map[storage.ShardID]*storage.Shard) (map[string]*table, []string, error) {
	if len(specs) == 0 {
		return nil, nil, fmt.Errorf("engine: no table declared; Cachet has no built-in table")
	}

	out := make(map[string]*table, len(specs))
	order := make([]string, 0, len(specs))

	for _, spec := range specs {
		if spec.Descriptor == nil {
			return nil, nil, fmt.Errorf("engine: a table was declared with no descriptor")
		}
		name := spec.Descriptor.Name
		if _, dup := out[name]; dup {
			// Two declarations of one table are two shapes for one set of cache entries, and the
			// entries carry a fingerprint of whichever shape happened to write them.
			return nil, nil, fmt.Errorf("engine: table %q is declared twice", name)
		}
		if len(spec.Shards) == 0 {
			return nil, nil, fmt.Errorf("engine: table %q names no shards", name)
		}

		owned := make(map[storage.ShardID]*storage.Shard, len(spec.Shards))
		for _, id := range spec.Shards {
			s, ok := shards[id]
			if !ok {
				// A shard in the topology with no connection would send a share of the key space
				// to a nil handle. Catching it here makes it a boot failure instead of a panic on
				// whichever request first hashes to the wrong place.
				return nil, nil, fmt.Errorf("engine: table %q is declared on shard %q, which has no connection", name, id)
			}
			// The shard has to know the table's statements, or the first read against it fails
			// naming a table the operator believes they declared.
			if _, err := s.Table(name); err != nil {
				return nil, nil, fmt.Errorf("engine: %w", err)
			}
			owned[id] = s
		}

		router, err := storage.NewRouter(spec.Shards)
		if err != nil {
			return nil, nil, fmt.Errorf("engine: table %q: %w", name, err)
		}
		out[name] = &table{d: spec.Descriptor, router: router, shards: owned}
		order = append(order, name)
	}
	return out, order, nil
}

// tableFor resolves the table a key names.
//
// Restricting keys to declared tables is deliberate rather than a shortcut: routing and cache
// identity are both derived from the key, so accepting an undeclared table would mean caching rows
// under an identity that no invalidation path knows how to produce.
func (e *Engine) tableFor(key schema.Key) (*table, error) {
	t, ok := e.tables[key.Table]
	if !ok {
		return nil, fmt.Errorf("%w: no table named %q is served by this engine", ErrInvalidRequest, key.Table)
	}
	if err := t.d.Validate(key); err != nil {
		return nil, errors.Join(ErrInvalidRequest, err)
	}
	return t, nil
}

// parseKey parses a key and resolves the table it names.
func (e *Engine) parseKey(raw string) (schema.Key, *table, error) {
	key, err := schema.ParseKey(raw)
	if err != nil {
		return schema.Key{}, nil, err
	}
	t, err := e.tableFor(key)
	if err != nil {
		return schema.Key{}, nil, err
	}
	return key, t, nil
}

// Table returns the declared shape of a table this engine serves.
//
// The proxy builds its patterns and its statements from the same declaration the engine caches
// against. Two declarations for one table would be two things to keep in step, and the failure
// mode of their drifting is the proxy answering a column list the engine's entries cannot fill.
func (e *Engine) Table(name string) (*schema.Descriptor, error) {
	t, ok := e.tables[name]
	if !ok {
		return nil, fmt.Errorf("engine: no table named %q is served by this engine", name)
	}
	return t.d, nil
}

// Tables returns every declared table, in declaration order.
func (e *Engine) Tables() []*schema.Descriptor {
	out := make([]*schema.Descriptor, 0, len(e.tableOrder))
	for _, name := range e.tableOrder {
		out = append(out, e.tables[name].d)
	}
	return out
}

// soleTable is the single declared table, for the paths that can only mean one.
//
// cachet.v1 describes rows with one table's columns as protocol fields, so it has nowhere to put a
// table name and cannot mean anything else. An engine serving several tables therefore cannot serve
// v1, and says so rather than picking one.
func (e *Engine) soleTable() (*table, error) {
	if len(e.tableOrder) != 1 {
		return nil, fmt.Errorf(
			"engine: this endpoint describes rows with one table's columns and cannot name a table; "+
				"%d are declared (%v). Use cachet.v2", len(e.tableOrder), e.tableOrder)
	}
	return e.tables[e.tableOrder[0]], nil
}
