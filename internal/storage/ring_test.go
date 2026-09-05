package storage_test

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("entities:%d", i)
	}
	return out
}

func TestRingWithNoNodesReturnsErrNoNodes(t *testing.T) {
	t.Parallel()

	r := storage.NewRing()

	_, err := r.Lookup("entities:1")
	if !errors.Is(err, storage.ErrNoNodes) {
		t.Errorf("Lookup on an empty ring returned %v, want ErrNoNodes", err)
	}
}

func TestRingRoutesEveryKeyToTheOnlyNode(t *testing.T) {
	t.Parallel()

	r := storage.NewRing("shard0")

	for _, k := range keys(1000) {
		got, err := r.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q) returned error: %v", k, err)
		}
		if got != "shard0" {
			t.Fatalf("Lookup(%q) = %q, want shard0", k, got)
		}
	}
}

func TestRingIsDeterministicAcrossInstances(t *testing.T) {
	t.Parallel()

	// Two engine processes must agree on routing without coordinating. If they disagree, one writes
	// a row to a shard the other never reads it from — a data-loss bug that no cache test would
	// catch. Insertion order must not matter either.
	a := storage.NewRing("shard0", "shard1", "shard2")
	b := storage.NewRing("shard2", "shard0", "shard1")

	for _, k := range keys(5000) {
		ka, err := a.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", k, err)
		}
		kb, err := b.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", k, err)
		}
		if ka != kb {
			t.Fatalf("rings disagree on %q: %q vs %q", k, ka, kb)
		}
	}
}

func TestRingDistributesKeysWithinFivePercent(t *testing.T) {
	t.Parallel()

	nodes := []string{"shard0", "shard1", "shard2"}
	r := storage.NewRing(nodes...)

	const total = 120_000
	counts := map[string]int{}
	for _, k := range keys(total) {
		n, err := r.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", k, err)
		}
		counts[n]++
	}

	// An uneven ring hot-spots one database shard under a uniform workload, which would quietly
	// invalidate every benchmark this project publishes.
	expected := float64(total) / float64(len(nodes))
	for _, n := range nodes {
		dev := math.Abs(float64(counts[n])-expected) / expected
		if dev > 0.05 {
			t.Errorf("node %s holds %d keys, expected ~%.0f (deviation %.1f%%, budget 5%%)",
				n, counts[n], expected, dev*100)
		}
	}
}

func TestAddingANodeMovesOnlyKeysDestinedForIt(t *testing.T) {
	t.Parallel()

	before := storage.NewRing("shard0", "shard1", "shard2")
	after := storage.NewRing("shard0", "shard1", "shard2", "shard3")

	const total = 60_000
	moved := 0
	for _, k := range keys(total) {
		b, err := before.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", k, err)
		}
		a, err := after.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", k, err)
		}
		if a == b {
			continue
		}
		moved++
		// This is the defining property of consistent hashing: a key that moves must move to the
		// NEW node. Anything else means adding capacity reshuffles unrelated traffic, and every
		// cache entry for those keys becomes a miss at once.
		if a != "shard3" {
			t.Fatalf("key %q moved from %s to %s; only moves to the new node are legitimate", k, b, a)
		}
	}

	// Adding a fourth node should relocate about a quarter of the key space.
	fraction := float64(moved) / float64(total)
	if fraction < 0.20 || fraction > 0.30 {
		t.Errorf("adding a node moved %.1f%% of keys, want 20–30%%", fraction*100)
	}
}

func TestRemovingANodeSpreadsItsKeysWithoutHotspotting(t *testing.T) {
	t.Parallel()

	before := storage.NewRing("shard0", "shard1", "shard2", "shard3")
	after := storage.NewRing("shard0", "shard1", "shard2")

	const total = 60_000
	inherited := map[string]int{}
	for _, k := range keys(total) {
		b, err := before.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", k, err)
		}
		a, err := after.Lookup(k)
		if err != nil {
			t.Fatalf("Lookup(%q): %v", k, err)
		}
		if b != "shard3" {
			// Keys not owned by the departed node must not move at all.
			if a != b {
				t.Fatalf("key %q moved from %s to %s although %s is still present", k, b, a, b)
			}
			continue
		}
		inherited[a]++
	}

	total3 := inherited["shard0"] + inherited["shard1"] + inherited["shard2"]
	if total3 == 0 {
		t.Fatal("no keys were inherited from the removed node")
	}
	// A dead node's load must spread, never land on one survivor. This is the same property that
	// lets the cache ring survive a Redis node loss without hot-spotting a database shard
	// (product spec §6, Tier 0).
	expected := float64(total3) / 3
	for _, n := range []string{"shard0", "shard1", "shard2"} {
		dev := math.Abs(float64(inherited[n])-expected) / expected
		if dev > 0.15 {
			t.Errorf("node %s inherited %d of %d orphaned keys, expected ~%.0f (deviation %.1f%%, budget 15%%)",
				n, inherited[n], total3, expected, dev*100)
		}
	}
}

func TestRingNodesAreReportedSorted(t *testing.T) {
	t.Parallel()

	r := storage.NewRing("shard2", "shard0", "shard1")

	got := r.Nodes()
	want := []string{"shard0", "shard1", "shard2"}
	if len(got) != len(want) {
		t.Fatalf("Nodes() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Nodes() = %v, want %v", got, want)
		}
	}
}

func TestRingIgnoresDuplicateNodes(t *testing.T) {
	t.Parallel()

	r := storage.NewRing("shard0", "shard1", "shard0")

	if got := len(r.Nodes()); got != 2 {
		t.Errorf("Nodes() has %d entries, want 2 — duplicates must not double a node's share", got)
	}
}

func TestRingLookupIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	r := storage.NewRing("shard0", "shard1", "shard2")
	ks := keys(2000)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, k := range ks {
				if _, err := r.Lookup(k); err != nil {
					t.Errorf("Lookup(%q): %v", k, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
