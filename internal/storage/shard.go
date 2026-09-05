package storage

import (
	"errors"
	"sort"
)

// ErrEmptyKey is returned when a caller asks to route an empty key. Routing one would succeed
// deterministically and be silently cacheable, which surfaces later as unexplained cross-talk
// rather than as an error at the point of the mistake.
var ErrEmptyKey = errors.New("storage: empty key")

// ShardID names one database shard. It is a distinct type rather than a string so that a shard
// name and a cache node name — which are routed by two independent rings — cannot be interchanged
// by accident.
type ShardID string

// Router maps keys to database shards.
//
// It wraps a Ring with the operations the engine actually performs, of which the important one is
// Group: a BatchGet issues one query per shard, not one per key. That is the difference between a
// batch API and a loop with extra steps.
//
// A Router is immutable and safe for concurrent use.
type Router struct {
	ring   *Ring
	shards map[ShardID]struct{}
}

// NewRouter builds a Router over the given shards. It returns ErrNoNodes if the topology is empty,
// because a Router that cannot route is a configuration error worth failing fast on rather than a
// zero value worth carrying around (CONTRIBUTING.md rule 15).
func NewRouter(shards []ShardID) (*Router, error) {
	if len(shards) == 0 {
		return nil, ErrNoNodes
	}

	names := make([]string, 0, len(shards))
	set := make(map[ShardID]struct{}, len(shards))
	for _, s := range shards {
		if s == "" {
			return nil, errors.New("storage: empty shard id in topology")
		}
		if _, dup := set[s]; dup {
			continue
		}
		set[s] = struct{}{}
		names = append(names, string(s))
	}
	sort.Strings(names)

	return &Router{ring: NewRing(names...), shards: set}, nil
}

// ShardFor returns the shard that owns key.
func (r *Router) ShardFor(key string) (ShardID, error) {
	if key == "" {
		return "", ErrEmptyKey
	}
	node, err := r.ring.Lookup(key)
	if err != nil {
		return "", err
	}
	return ShardID(node), nil
}

// Group partitions keys by owning shard, de-duplicating repeats and preserving input order within
// each group so a caller can zip results back against its own request slice without sorting.
func (r *Router) Group(keys []string) (map[ShardID][]string, error) {
	groups := make(map[ShardID][]string)
	seen := make(map[string]struct{}, len(keys))

	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}

		id, err := r.ShardFor(k)
		if err != nil {
			return nil, err
		}
		groups[id] = append(groups[id], k)
	}
	return groups, nil
}

// Has reports whether id is part of this topology.
func (r *Router) Has(id ShardID) bool {
	_, ok := r.shards[id]
	return ok
}

// Shards returns the topology in sorted order.
func (r *Router) Shards() []ShardID {
	out := make([]ShardID, 0, len(r.shards))
	for _, n := range r.ring.Nodes() {
		out = append(out, ShardID(n))
	}
	return out
}
