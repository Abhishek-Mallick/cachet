package cache_test

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

func cacheKeys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("entities:%d", i)
	}
	return out
}

func newRouter(t *testing.T, addrs ...string) *cache.Router {
	t.Helper()
	r, err := cache.NewRouter(addrs)
	if err != nil {
		t.Fatalf("NewRouter(%v): %v", addrs, err)
	}
	return r
}

func TestRouterRejectsAnEmptyNodeList(t *testing.T) {
	t.Parallel()

	if _, err := cache.NewRouter(nil); !errors.Is(err, cache.ErrNoNodes) {
		t.Errorf("NewRouter(nil) returned %v, want ErrNoNodes", err)
	}
}

func TestRouterRejectsAnEmptyNodeAddress(t *testing.T) {
	t.Parallel()

	if _, err := cache.NewRouter([]string{"10.0.0.1:6379", ""}); err == nil {
		t.Error("NewRouter accepted an empty node address; a blank address routes deterministically to nothing")
	}
}

func TestRouterRejectsAnEmptyKey(t *testing.T) {
	t.Parallel()

	r := newRouter(t, "10.0.0.1:6379", "10.0.0.2:6379")
	if _, err := r.NodeFor(""); !errors.Is(err, cache.ErrEmptyKey) {
		t.Errorf("NodeFor(\"\") returned %v, want ErrEmptyKey", err)
	}
}

func TestRouterRoutesEveryKeyToAConfiguredNode(t *testing.T) {
	t.Parallel()

	addrs := []string{"10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379"}
	r := newRouter(t, addrs...)

	known := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		known[a] = true
	}

	for _, k := range cacheKeys(500) {
		node, err := r.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%q): %v", k, err)
		}
		if !known[node] {
			t.Fatalf("NodeFor(%q) = %q, which is not a configured node", k, node)
		}
	}
}

func TestRouterIsDeterministicAcrossInstances(t *testing.T) {
	t.Parallel()

	// Two engines given the same node set in different orders must route identically. Without this
	// a sidecar fleet would split its cache into per-process views of the same keys, and the hit
	// rate would silently depend on how each process read its config.
	a := newRouter(t, "10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379")
	b := newRouter(t, "10.0.0.3:6379", "10.0.0.1:6379", "10.0.0.2:6379")

	for _, k := range cacheKeys(500) {
		got, err := a.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%q): %v", k, err)
		}
		want, err := b.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%q): %v", k, err)
		}
		if got != want {
			t.Fatalf("node order changed routing for %q: %q vs %q", k, got, want)
		}
	}
}

func TestRouterSpreadsKeysWithinFivePercent(t *testing.T) {
	t.Parallel()

	addrs := []string{"10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379"}
	r := newRouter(t, addrs...)

	const total = 30000
	counts := make(map[string]int, len(addrs))
	for _, k := range cacheKeys(total) {
		node, err := r.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%q): %v", k, err)
		}
		counts[node]++
	}

	// An uneven cache ring concentrates a disproportionate share of misses on one node, which is
	// the same hot-spot the independent ring exists to prevent — just moved one layer over.
	ideal := float64(total) / float64(len(addrs))
	for node, got := range counts {
		if drift := math.Abs(float64(got)-ideal) / ideal; drift > 0.05 {
			t.Errorf("node %s holds %d keys, ideal %.0f (%.1f%% off; budget 5%%)", node, got, ideal, drift*100)
		}
	}
}

func TestLosingANodeMovesOnlyItsOwnKeys(t *testing.T) {
	t.Parallel()

	// This is the whole reason for consistent hashing here. When a cache node dies, the keys it
	// held must be redistributed across the survivors and every other key must stay put. A plain
	// modulo would remap almost everything, turning one node's failure into a full-cache miss
	// storm against the database — an availability incident caused by the cache, not survived by it.
	before := newRouter(t, "10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379")
	after := newRouter(t, "10.0.0.1:6379", "10.0.0.2:6379")

	moved, lost := 0, 0
	all := cacheKeys(30000)
	for _, k := range all {
		b, err := before.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%q): %v", k, err)
		}
		a, err := after.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%q): %v", k, err)
		}
		if b != a {
			moved++
			if b == "10.0.0.3:6379" {
				lost++
			}
		}
	}

	if moved != lost {
		t.Errorf("%d keys moved but only %d belonged to the removed node; %d keys were reshuffled needlessly",
			moved, lost, moved-lost)
	}
}

func TestCacheRoutingIsIndependentOfShardRouting(t *testing.T) {
	t.Parallel()

	// The design requirement from product spec §6 (Tier 0), stated as a test: cache placement and
	// database placement must not be correlated. If they were, losing a cache node would send that
	// node's entire miss load to one database shard instead of spreading it — the hot-spot the
	// separation exists to prevent.
	//
	// Asserting independence directly is what keeps a future "just reuse the shard router" edit
	// from silently reintroducing the coupling.
	r := newRouter(t, "10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379")
	shards, err := storage.NewRouter([]storage.ShardID{"shard0", "shard1", "shard2"})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	// pair[cacheNode][shard] counts keys landing on that combination. If the two rings agreed,
	// every cache node would map to exactly one shard and two of the three columns would be zero.
	pair := make(map[string]map[storage.ShardID]int)
	all := cacheKeys(30000)
	for _, k := range all {
		node, err := r.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%q): %v", k, err)
		}
		shard, err := shards.ShardFor(k)
		if err != nil {
			t.Fatalf("ShardFor(%q): %v", k, err)
		}
		if pair[node] == nil {
			pair[node] = make(map[storage.ShardID]int)
		}
		pair[node][shard]++
	}

	for node, byShard := range pair {
		if len(byShard) < 3 {
			t.Fatalf("cache node %s draws from only %d shards; the two rings are correlated", node, len(byShard))
		}
		total := 0
		for _, n := range byShard {
			total += n
		}
		// Each cache node's keys should be spread across the shards in roughly equal thirds.
		ideal := float64(total) / 3
		for shard, n := range byShard {
			if drift := math.Abs(float64(n)-ideal) / ideal; drift > 0.10 {
				t.Errorf("cache node %s sends %.1f%% of its keys to %s (expected ~33%%): the rings are correlated",
					node, float64(n)/float64(total)*100, shard)
			}
		}
	}
}

func TestRouterNodesAreSortedAndDeduplicated(t *testing.T) {
	t.Parallel()

	r := newRouter(t, "10.0.0.2:6379", "10.0.0.1:6379", "10.0.0.2:6379")
	got := r.Nodes()
	want := []string{"10.0.0.1:6379", "10.0.0.2:6379"}

	if len(got) != len(want) {
		t.Fatalf("Nodes() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Nodes() = %v, want %v", got, want)
		}
	}
}
