//go:build integration

package cache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/Abhishek-Mallick/cachet/internal/breaker"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
)

// These tests take a real cache node away mid-flight. The breaker's whole purpose is what happens
// when a node stops answering, and that cannot be observed against a healthy stack: a mock that
// returns errors on demand would be testing the mock's idea of failure, not a dead TCP endpoint.

// startPair brings up two fresh cache nodes and returns their addresses plus the container backing
// the SECOND one, which the caller kills. They are dedicated to the caller rather than shared,
// because these tests kill nodes and a killed node must not leak into another test's ring.
func startPair(t *testing.T) ([]string, testcontainers.Container) {
	t.Helper()

	var addrs []string
	var containers []testcontainers.Container
	for i := 0; i < 2; i++ {
		addr, c := startNode(t, i)
		addrs = append(addrs, addr)
		containers = append(containers, c)
	}
	t.Cleanup(func() { terminateAll(containers) })

	return addrs, containers[1]
}

// startNode owns the lifetime of its own startup context. Container lifecycle is deliberately kept
// off the test's context: a cancelled test must still be able to start, stop and reap its
// containers, or a failed run leaves nodes behind that poison the next one.
func startNode(t *testing.T, i int) (string, testcontainers.Container) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	addr, c, err := startValkey(ctx)
	if err != nil {
		t.Fatalf("start cache node %d: %v", i, err)
	}
	return addr, c
}

// terminateAll and killNode deliberately build their own context instead of taking the test's.
// Teardown and mid-test kills have to run even when the test's context is already cancelled; using
// a cancelled context would abandon the containers, which then survive the run and poison the next
// one with a stale ring member.
func terminateAll(containers []testcontainers.Container) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, c := range containers {
		_ = c.Terminate(ctx)
	}
}

func killNode(t *testing.T, c testcontainers.Container) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Stop(ctx, nil); err != nil {
		t.Fatalf("stop cache node: %v", err)
	}
}

// keysOn returns n keys that route to the given node.
func keysOn(t *testing.T, r *cache.Router, node string, n int) []string {
	t.Helper()

	var out []string
	for i := 0; len(out) < n; i++ {
		k := fmt.Sprintf("breaker:%d", i)
		owner, err := r.NodeFor(k)
		if err != nil {
			t.Fatalf("NodeFor(%s): %v", k, err)
		}
		if owner == node {
			out = append(out, k)
		}
		if i > 100000 {
			t.Fatalf("could not find %d keys routing to %s", n, node)
		}
	}
	return out
}

// blackholeAddr is TEST-NET-3 (RFC 5737): reserved, unrouteable, and it drops packets rather than
// refusing them — the behaviour a stopped container has on a Linux host.
const blackholeAddr = "203.0.113.1:6379"

func fastBreaker() breaker.Options {
	return breaker.Options{
		Window:       10 * time.Second,
		Buckets:      10,
		MinRequests:  10,
		FailureFloor: 0.05,
		MaxShed:      0.95,
	}
}

func TestReadsToADeadNodeSurfaceTheErrorUntilShedding(t *testing.T) {
	ctx := context.Background()
	addrs, victim := startPair(t)

	c, err := cache.New(ctx, cache.Options{
		Addresses: addrs,
		TTL:       time.Hour,
		Timeout:   200 * time.Millisecond,
		Breaker:   fastBreaker(),
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	router, err := cache.NewRouter(addrs)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	dead := addrs[1]
	keys := keysOn(t, router, dead, 40)

	killNode(t, victim)

	// Two different things must be distinguishable, and this test pins both.
	//
	// A read that was ATTEMPTED and failed returns an error. The engine degrades it to a miss and
	// falls through to the database, but it also logs it and counts it as
	// cache_operations_total{op=get,result=error}. If the client folded it into a plain miss, a node
	// outage would look exactly like a cold cache on every dashboard, and the breaker would be
	// shedding traffic for a reason nobody could see.
	//
	// A read that was SHED returns a clean miss, because nothing was attempted and there is nothing
	// to report.
	errored, shed := 0, 0
	for _, k := range keys {
		_, hit, err := c.Get(ctx, k)
		if hit {
			t.Fatalf("Get(%s) reported a hit from a dead node", k)
		}
		if err != nil {
			errored++
		} else {
			shed++
		}
	}

	if errored == 0 {
		t.Error("no read to the dead node surfaced an error; a node outage would be invisible in metrics")
	}
	if shed == 0 {
		t.Error("no read to the dead node was shed; the breaker never opened")
	}
}

func TestTrafficToADeadNodeIsProgressivelyShed(t *testing.T) {
	ctx := context.Background()
	addrs, victim := startPair(t)

	c, err := cache.New(ctx, cache.Options{
		Addresses: addrs,
		TTL:       time.Hour,
		Timeout:   200 * time.Millisecond,
		Breaker:   fastBreaker(),
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	router, err := cache.NewRouter(addrs)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	dead, alive := addrs[1], addrs[0]
	deadKeys := keysOn(t, router, dead, 60)

	killNode(t, victim)
	for _, k := range deadKeys {
		_, _, _ = c.Get(ctx, k)
	}

	// The point of the breaker: stop paying a timeout to a node that is not answering.
	stats := c.BreakerStats()
	if s, ok := stats[dead]; !ok || s.ShedRate == 0 {
		t.Errorf("the dead node %s is not being shed: %+v", dead, s)
	}
	// And the point of it being per-node: one dead node must not cost the healthy one its traffic.
	if s, ok := stats[alive]; ok && s.ShedRate != 0 {
		t.Errorf("the healthy node %s is being shed at %.3f because a different node died", alive, s.ShedRate)
	}
}

func TestShedReadsSkipTheNodeEntirely(t *testing.T) {
	ctx := context.Background()
	addrs, victim := startPair(t)

	c, err := cache.New(ctx, cache.Options{
		Addresses: addrs,
		TTL:       time.Hour,
		Timeout:   500 * time.Millisecond,
		Breaker:   fastBreaker(),
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	router, err := cache.NewRouter(addrs)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	dead := addrs[1]
	keys := keysOn(t, router, dead, 200)

	killNode(t, victim)

	// Warm the breaker until it is shedding most traffic.
	for _, k := range keys[:60] {
		_, _, _ = c.Get(ctx, k)
	}
	if s := c.BreakerStats()[dead]; s.ShedRate < 0.5 {
		t.Fatalf("precondition: expected heavy shedding, got %+v", s)
	}

	// Shed reads must return immediately rather than waiting for a connection that will not come.
	// Saving that timeout is the entire latency argument for the breaker; if a shed read still
	// blocked, the breaker would be pure overhead.
	start := time.Now()
	for _, k := range keys[60:160] {
		_, _, _ = c.Get(ctx, k)
	}
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Errorf("100 mostly-shed reads took %s; shed reads are still waiting on the dead node", elapsed)
	}
}

func TestTombstonesAreNeverShed(t *testing.T) {
	ctx := context.Background()
	addrs, victim := startPair(t)

	c, err := cache.New(ctx, cache.Options{
		Addresses: addrs,
		TTL:       time.Hour,
		Timeout:   200 * time.Millisecond,
		Breaker:   fastBreaker(),
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	router, err := cache.NewRouter(addrs)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	dead := addrs[1]
	keys := keysOn(t, router, dead, 60)

	killNode(t, victim)
	for _, k := range keys[:50] {
		_, _, _ = c.Get(ctx, k)
	}
	if s := c.BreakerStats()[dead]; s.ShedRate < 0.5 {
		t.Fatalf("precondition: expected heavy shedding, got %+v", s)
	}

	// Shedding a READ costs hit rate. Shedding an INVALIDATION costs correctness: the entry
	// survives, and the cache goes on serving a value the database has already changed. So the
	// breaker gates reads and fills, never tombstones — and a tombstone that cannot be applied must
	// be reported as an error, never silently dropped.
	failures := 0
	for _, k := range keys[50:] {
		applied, err := c.Tombstone(ctx, k, 99)
		if err != nil {
			failures++
			continue
		}
		if !applied {
			t.Errorf("Tombstone(%s) reported success without applying", k)
		}
	}
	if failures != len(keys[50:]) {
		t.Errorf("%d of %d tombstones to a dead node were silently swallowed; every one must surface an error",
			len(keys[50:])-failures, len(keys[50:]))
	}
}

func TestHealthyNodesAreNeverShed(t *testing.T) {
	ctx := context.Background()
	addrs := ringCluster(ctx, t)

	c, err := cache.New(ctx, cache.Options{
		Addresses: addrs,
		TTL:       time.Hour,
		Breaker:   fastBreaker(),
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("healthy:%d", i)
		if _, err := c.Fill(ctx, k, cache.Entry{RowVersion: uint64(i + 1), FillVersion: uint64(i + 1)}); err != nil {
			t.Fatalf("Fill(%s): %v", k, err)
		}
		if _, _, err := c.Get(ctx, k); err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
	}

	for node, s := range c.BreakerStats() {
		if s.ShedRate != 0 {
			t.Errorf("healthy node %s is being shed at %.3f: %+v", node, s.ShedRate, s)
		}
	}
}

// TestConnectingToAnUnreachableNodeFailsQuickly pins a bound that was missing entirely.
//
// The connection pool hardcoded a 2-second dial timeout while ReadTimeout and WriteTimeout honoured
// the configured value, and go-redis retried internally on top of that. One attempt against a node
// that BLACKHOLES packets therefore cost tens of seconds — measured at ~30 s before this fix.
//
// Two things were wrong with that, and neither is test-only:
//
//   - An engine with one mistyped cache address hangs for half a minute before saying so. Boot
//     failures must be fast and legible; a slow one reads as a hang.
//   - Options.Timeout documents that it "bounds a single cache operation". It did not. The whole
//     argument for the circuit breaker is that it stops paying timeouts to a node that will not
//     answer, and an unbounded dial undercuts exactly that.
//
// 203.0.113.1 is TEST-NET-3 (RFC 5737): reserved, unrouteable, and it DROPS packets rather than
// refusing them — the behaviour a stopped container has on a Linux host. A stopped container
// refuses fast on Docker Desktop, which is why this cost was invisible on a laptop and showed up
// in CI.
func TestConnectingToAnUnreachableNodeFailsQuickly(t *testing.T) {
	ctx := context.Background()
	addrs, _ := startPair(t)

	const timeout = 200 * time.Millisecond
	start := time.Now()
	c, err := cache.New(ctx, cache.Options{
		Addresses: append(addrs, blackholeAddr),
		TTL:       time.Hour,
		Timeout:   timeout,
		Breaker:   fastBreaker(),
	})
	elapsed := time.Since(start)

	if err == nil {
		_ = c.Close()
		t.Fatal("cache.New succeeded with an unreachable node in the ring; a client that boots with " +
			"part of its ring dead routes that share of the key space into errors")
	}

	// Generous headroom over the configured timeout: the assertion is that a bound EXISTS, not that
	// it is tight. Before the fix this took roughly thirty seconds.
	if elapsed > 5*time.Second {
		t.Errorf("cache.New took %s to report an unreachable node with a %s timeout configured; "+
			"the dial is not bounded by the operation timeout", elapsed, timeout)
	}
	t.Logf("boot failed in %s: %v", elapsed.Round(time.Millisecond), err)
}
