package cache

import (
	"errors"
	"fmt"
	"sort"

	"github.com/Abhishek-Mallick/cachet/internal/hashring"
)

// ErrNoNodes is returned when a Router is built over an empty node set.
var ErrNoNodes = hashring.ErrNoNodes

// ErrEmptyKey is returned when a caller asks to route an empty key. Routing one would succeed
// deterministically and be silently cacheable, which surfaces later as unexplained cross-talk
// rather than as an error at the point of the mistake.
var ErrEmptyKey = errors.New("cache: empty key")

// Router maps cache keys to cache nodes.
//
// It is deliberately a separate type from storage.Router, over a separate hashring.Ring, built from
// a separate config field — and neither package imports the other. That separation is a stated
// product requirement, not a style preference (product spec §6, Tier 0): if cache placement were
// derived from database placement, every key held by a failed cache node would miss to the same
// database shard. One node's failure would become one shard's overload, which is precisely the
// hot-spot a cache is supposed to prevent. Spread across an independent ring, those misses land on
// every shard in proportion to the data, and the database sees a uniform bump instead of a spike.
//
// TestCacheRoutingIsIndependentOfShardRouting asserts the independence directly, so that a future
// "just reuse the shard router" edit fails a test rather than quietly changing the failure mode.
//
// A Router is immutable after construction and safe for concurrent use.
type Router struct {
	ring  *hashring.Ring
	nodes []string
}

// NewRouter builds a Router over the given cache node addresses.
//
// Duplicates are ignored and order does not matter: two engines handed the same set in any order
// route identically, which is what lets a sidecar fleet share one cache without coordinating.
func NewRouter(nodes []string) (*Router, error) {
	if len(nodes) == 0 {
		return nil, ErrNoNodes
	}

	seen := make(map[string]struct{}, len(nodes))
	unique := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n == "" {
			return nil, errors.New("cache: empty node address in cache topology")
		}
		if _, dup := seen[n]; dup {
			continue
		}
		seen[n] = struct{}{}
		unique = append(unique, n)
	}
	sort.Strings(unique)

	return &Router{ring: hashring.NewRing(unique...), nodes: unique}, nil
}

// NodeFor returns the cache node that owns key.
func (r *Router) NodeFor(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}
	node, err := r.ring.Lookup(key)
	if err != nil {
		return "", fmt.Errorf("cache: route %s: %w", key, err)
	}
	return node, nil
}

// Nodes returns the cache nodes in sorted order. The returned slice is a copy.
func (r *Router) Nodes() []string {
	out := make([]string, len(r.nodes))
	copy(out, r.nodes)
	return out
}
