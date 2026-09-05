package storage

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
)

// ErrNoNodes is returned by Lookup when the ring holds no nodes. It is a sentinel rather than an
// ad-hoc error because callers legitimately branch on it during startup, before shard discovery
// has completed.
var ErrNoNodes = errors.New("storage: ring has no nodes")

// ringVirtualNodes is how many points each node occupies on the ring.
//
// The number is a distribution-quality knob, not a tuning parameter: too few and the key space
// splits unevenly, which hot-spots a database shard under a uniform workload and quietly
// invalidates every benchmark. 256 keeps three nodes within roughly 2% of even, comfortably inside
// the 5% budget asserted by TestRingDistributesKeysWithinFivePercent.
const ringVirtualNodes = 256

// Ring maps keys to nodes by consistent hashing.
//
// Cachet uses two independent Rings, and keeping them independent is a design requirement rather
// than an accident: one routes keys to database shards, the other routes cache entries to cache
// nodes. Sharing a ring would mean the loss of one cache node concentrates its misses onto a single
// database shard — the exact hot-spot the separation exists to prevent (product spec §6, Tier 0).
//
// A Ring is immutable after construction and therefore safe for concurrent use without locking.
// Changing membership means building a new Ring and swapping the pointer, which also makes
// "what did routing look like when this key was written?" an answerable question.
type Ring struct {
	// points holds the hash of every virtual node, sorted ascending.
	points []uint64
	// owner maps a point back to the node that owns it. Parallel to points by index.
	owner []string
	// nodes is the sorted, de-duplicated node list.
	nodes []string
}

// NewRing builds a Ring over the given nodes. Duplicate names are ignored, and node order does not
// affect routing: two processes given the same set in any order route identically, which is what
// lets engine instances agree without coordinating.
func NewRing(nodes ...string) *Ring {
	unique := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		unique[n] = struct{}{}
	}

	sorted := make([]string, 0, len(unique))
	for n := range unique {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)

	r := &Ring{
		points: make([]uint64, 0, len(sorted)*ringVirtualNodes),
		owner:  make([]string, 0, len(sorted)*ringVirtualNodes),
		nodes:  sorted,
	}

	type placement struct {
		point uint64
		node  string
	}
	placements := make([]placement, 0, cap(r.points))
	for _, n := range sorted {
		for i := 0; i < ringVirtualNodes; i++ {
			placements = append(placements, placement{
				point: hashKey(n + "#" + strconv.Itoa(i)),
				node:  n,
			})
		}
	}
	sort.Slice(placements, func(i, j int) bool {
		if placements[i].point != placements[j].point {
			return placements[i].point < placements[j].point
		}
		// Ties are astronomically unlikely with a 64-bit hash, but ordering them by node name keeps
		// construction deterministic if one ever occurs.
		return placements[i].node < placements[j].node
	})

	for _, p := range placements {
		r.points = append(r.points, p.point)
		r.owner = append(r.owner, p.node)
	}
	return r
}

// Lookup returns the node that owns key: the first virtual node clockwise from the key's hash,
// wrapping at the end of the ring.
func (r *Ring) Lookup(key string) (string, error) {
	if len(r.points) == 0 {
		return "", ErrNoNodes
	}

	h := hashKey(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if i == len(r.points) {
		i = 0 // wrap around
	}
	return r.owner[i], nil
}

// Nodes returns the ring's nodes in sorted order. The returned slice is a copy; the ring is
// immutable and intends to stay that way.
func (r *Ring) Nodes() []string {
	out := make([]string, len(r.nodes))
	copy(out, r.nodes)
	return out
}

// hashKey hashes a string to a ring position.
//
// FNV-1a alone is not good enough here, and the ring tests proved it: on near-identical inputs —
// which is exactly what "shard0#0", "shard0#1", … and "entities:1", "entities:2", … are — FNV-1a's
// avalanche is weak, its outputs cluster, and the key space splits 65/33/22 instead of evenly.
// That would have hot-spotted a database shard under a uniform workload and quietly invalidated
// every benchmark this project publishes.
//
// So the FNV digest is passed through the SplitMix64 finalizer, whose only job is avalanche: a
// one-bit input change flips about half the output bits. FNV supplies a cheap digest of arbitrary
// input; the finalizer makes it uniform. Both are stdlib-only and neither appears in a profile.
//
// This is not a cryptographic hash and does not need to be. An adversary who can choose keys can
// at worst unbalance their own traffic.
func hashKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // hash.Hash writes never fail
	return splitMix64(h.Sum64())
}

// splitMix64 is the finalizing mix from Sebastiano Vigna's SplitMix64 generator. It is a bijection,
// so it cannot introduce collisions that were not already present in the digest it is given.
func splitMix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}
