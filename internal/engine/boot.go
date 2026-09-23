package engine

import (
	"context"
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// Boot: turning a declaration into something that can serve traffic.
//
// Three things have to happen, in this order, before a single request is accepted: connect, read
// INFORMATION_SCHEMA, verify the declaration against it. Doing any of it lazily would move a
// configuration error from startup to the first user request, which converts an operator problem
// into a customer problem.
//
// It lives here rather than in each binary because the engine, the proxy and the test harness must
// all reach the same verdict about the same config. Three copies of this would be three chances to
// disagree about what a deployment declared.

// OpenTables verifies every declared table against every shard it lives on and attaches the
// statements to those shards.
//
// The returned specs are what Options.Tables takes.
func OpenTables(ctx context.Context, cfg config.Config, shards map[storage.ShardID]*storage.Shard) ([]TableSpec, error) {
	descriptors, err := cfg.Descriptors()
	if err != nil {
		return nil, err
	}

	specs := make([]TableSpec, 0, len(descriptors))
	for _, d := range descriptors {
		declared, ok := cfg.TableNamed(d.Name)
		if !ok {
			return nil, fmt.Errorf("engine: table %q vanished between validation and boot", d.Name)
		}
		topology, err := cfg.ShardsFor(d.Name)
		if err != nil {
			return nil, err
		}
		if len(topology) == 0 {
			return nil, fmt.Errorf("engine: table %q names a topology with no shards", d.Name)
		}

		ids := make([]storage.ShardID, 0, len(topology))
		for _, sh := range topology {
			id := storage.ShardID(sh.ID)
			shard, ok := shards[id]
			if !ok {
				return nil, fmt.Errorf("engine: table %q is declared on shard %q, which has no connection", d.Name, id)
			}

			// Introspected per shard, not once. A table that exists on two shards with different
			// shapes is a half-migrated deployment, and the shard that was missed is exactly the
			// one that would serve wrong rows.
			live, indexes, err := shard.Introspect(ctx, d.Name)
			if err != nil {
				return nil, fmt.Errorf("engine: shard %s: %w", id, err)
			}
			if err := storage.VerifyAgainstLive(d, live); err != nil {
				return nil, fmt.Errorf("engine: shard %s: %w", id, err)
			}

			t, err := storage.NewTable(d, indexes, declared.PredicateSpecs()...)
			if err != nil {
				return nil, fmt.Errorf("engine: shard %s: %w", id, err)
			}
			shard.WithTables(t)
			ids = append(ids, id)
		}
		specs = append(specs, TableSpec{Descriptor: d, Shards: ids})
	}
	return specs, nil
}

// UndeclaredColumnsOn reports the live columns a table has that the declaration does not, using the
// first shard the table lives on.
//
// It is what tells the proxy whether a cache entry is the whole row, and therefore whether
// `SELECT *` can be answered from one. Empty means it can.
func UndeclaredColumnsOn(ctx context.Context, cfg config.Config, shards map[storage.ShardID]*storage.Shard, name string) ([]string, error) {
	d, err := descriptorNamed(cfg, name)
	if err != nil {
		return nil, err
	}
	topology, err := cfg.ShardsFor(name)
	if err != nil {
		return nil, err
	}
	if len(topology) == 0 {
		return nil, fmt.Errorf("engine: table %q names a topology with no shards", name)
	}
	shard, ok := shards[storage.ShardID(topology[0].ID)]
	if !ok {
		return nil, fmt.Errorf("engine: shard %q has no connection", topology[0].ID)
	}

	live, _, err := shard.Introspect(ctx, name)
	if err != nil {
		return nil, err
	}
	return storage.UndeclaredColumns(d, live), nil
}

func descriptorNamed(cfg config.Config, name string) (*schema.Descriptor, error) {
	descriptors, err := cfg.Descriptors()
	if err != nil {
		return nil, err
	}
	for _, d := range descriptors {
		if d.Name == name {
			return d, nil
		}
	}
	return nil, fmt.Errorf("engine: no table named %q is declared", name)
}
