//go:build integration

package cache_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
)

// The independent cache ring needs more than one cache node to mean anything. These tests run
// against three real Valkey containers, because the property under test — that an entry lives on
// exactly the node the ring names, and that losing a node costs only that node's keys — cannot be
// observed against a single server no matter how the client is configured.

var (
	ringOnce  sync.Once
	ringAddrs []string
	ringNodes []testcontainers.Container
	ringErr   error
)

const ringNodeCount = 3

func ringCluster(ctx context.Context, t *testing.T) []string {
	t.Helper()

	ringOnce.Do(func() {
		for i := 0; i < ringNodeCount; i++ {
			addr, c, err := startValkey(ctx)
			if err != nil {
				ringErr = fmt.Errorf("start cache node %d: %w", i, err)
				return
			}
			ringAddrs = append(ringAddrs, addr)
			ringNodes = append(ringNodes, c)
		}
	})
	if ringErr != nil {
		// Fatal, never skip. A gate that goes green while testing nothing is worse than no gate
		// (test/README.md).
		t.Fatalf("cache ring cluster: %v", ringErr)
	}
	return ringAddrs
}

func tearDownRing() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, c := range ringNodes {
		_ = c.Terminate(ctx)
	}
}

func newRingClient(ctx context.Context, t *testing.T, addrs []string) *cache.Client {
	t.Helper()

	c, err := cache.New(ctx, cache.Options{Addresses: addrs, TTL: time.Hour})
	if err != nil {
		t.Fatalf("cache.New(%v): %v", addrs, err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

// countedOn asks one specific node directly how many of the given keys it holds, bypassing the
// client entirely. Going around the abstraction is the point: if the assertion used the client to
// check the client's own placement, it would pass even if every key landed on one node.
func countedOn(ctx context.Context, t *testing.T, addr string, keys []string) int {
	t.Helper()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()

	n := 0
	for _, k := range keys {
		exists, err := rdb.Exists(ctx, k).Result()
		if err != nil {
			t.Fatalf("EXISTS %s on %s: %v", k, addr, err)
		}
		n += int(exists)
	}
	return n
}

func TestEntriesAreStoredOnTheNodeTheRingNames(t *testing.T) {
	ctx := context.Background()
	addrs := ringCluster(ctx, t)
	c := newRingClient(ctx, t, addrs)

	router, err := cache.NewRouter(addrs)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	keys := cacheKeys(300)
	for i, k := range keys {
		if _, err := c.Fill(ctx, k, cache.Entry{RowVersion: uint64(i + 1), FillVersion: uint64(i + 1)}); err != nil {
			t.Fatalf("Fill(%s): %v", k, err)
		}
	}

	for _, k := range keys {
		want, err := router.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%s): %v", k, err)
		}
		for _, addr := range addrs {
			got := countedOn(ctx, t, addr, []string{k})
			if addr == want && got != 1 {
				t.Fatalf("key %s should live on %s but is not there", k, want)
			}
			if addr != want && got != 0 {
				t.Fatalf("key %s should live only on %s but was also found on %s", k, want, addr)
			}
		}
	}
}

func TestEveryCacheNodeReceivesTraffic(t *testing.T) {
	ctx := context.Background()
	addrs := ringCluster(ctx, t)
	c := newRingClient(ctx, t, addrs)

	// A client that routed everything to Addresses[0] would still pass a round-trip test. This is
	// the assertion that catches it.
	keys := make([]string, 600)
	for i := range keys {
		keys[i] = fmt.Sprintf("spread:%d", i)
	}
	for i, k := range keys {
		if _, err := c.Fill(ctx, k, cache.Entry{RowVersion: uint64(i + 1), FillVersion: uint64(i + 1)}); err != nil {
			t.Fatalf("Fill(%s): %v", k, err)
		}
	}

	for _, addr := range addrs {
		if got := countedOn(ctx, t, addr, keys); got == 0 {
			t.Errorf("cache node %s holds none of the %d keys; the ring is not being used", addr, len(keys))
		}
	}
}

func TestReadsAndWritesAgreeOnPlacement(t *testing.T) {
	ctx := context.Background()
	addrs := ringCluster(ctx, t)
	c := newRingClient(ctx, t, addrs)

	// Fill, Get and Tombstone must each route a key to the same node. If they disagreed, a write
	// would land on one node and its invalidation on another — the entry would survive its own
	// tombstone, and the cache would serve data it had been told to drop.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("agree:%d", i)

		if _, err := c.Fill(ctx, key, cache.Entry{RowVersion: 10, FillVersion: 10, Row: []byte("v1")}); err != nil {
			t.Fatalf("Fill(%s): %v", key, err)
		}
		if _, hit, err := c.Get(ctx, key); err != nil || !hit {
			t.Fatalf("Get(%s) after Fill: hit=%v err=%v", key, hit, err)
		}
		if applied, err := c.Tombstone(ctx, key, 20); err != nil || !applied {
			t.Fatalf("Tombstone(%s): applied=%v err=%v", key, applied, err)
		}
		if _, hit, err := c.Get(ctx, key); err != nil || hit {
			t.Fatalf("Get(%s) after Tombstone: hit=%v err=%v, want a miss", key, hit, err)
		}
	}
}

func TestFlushClearsEveryNode(t *testing.T) {
	ctx := context.Background()
	addrs := ringCluster(ctx, t)
	c := newRingClient(ctx, t, addrs)

	keys := make([]string, 300)
	for i := range keys {
		keys[i] = fmt.Sprintf("flush:%d", i)
	}
	for i, k := range keys {
		if _, err := c.Fill(ctx, k, cache.Entry{RowVersion: uint64(i + 1), FillVersion: uint64(i + 1)}); err != nil {
			t.Fatalf("Fill(%s): %v", k, err)
		}
	}

	if err := c.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// A Flush that cleared only the first node would leave the operator believing the cache was
	// empty while two thirds of it still served entries.
	for _, addr := range addrs {
		if got := countedOn(ctx, t, addr, keys); got != 0 {
			t.Errorf("cache node %s still holds %d keys after Flush", addr, got)
		}
	}
}

func TestClientReportsItsNodes(t *testing.T) {
	ctx := context.Background()
	addrs := ringCluster(ctx, t)
	c := newRingClient(ctx, t, addrs)

	// cachetctl needs to answer "which node holds this key?" without reimplementing the ring.
	if got := len(c.Nodes()); got != len(addrs) {
		t.Errorf("Nodes() returned %d nodes, want %d", got, len(addrs))
	}
	node, err := c.NodeFor("entities:1")
	if err != nil {
		t.Fatalf("NodeFor: %v", err)
	}
	found := false
	for _, a := range addrs {
		if a == node {
			found = true
		}
	}
	if !found {
		t.Errorf("NodeFor returned %q, which is not a configured node", node)
	}
}
