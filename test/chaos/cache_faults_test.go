//go:build chaos

package chaos_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/breaker"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

func itoa(i int) string { return strconv.Itoa(i) }

// breakerLine renders breaker state the way an operator reads it, rather than dumping a Go map into
// a document somebody is meant to learn from.
func breakerLine(stats map[string]breaker.Stats) string {
	nodes := make([]string, 0, len(stats))
	for node := range stats {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		s := stats[node]
		parts = append(parts, fmt.Sprintf("`%s` %d/%d failing (%.0f%%), shedding %.0f%%",
			node, s.Failures, s.Total, s.FailureRate*100, s.ShedRate*100))
	}
	return strings.Join(parts, "; ")
}

// TestFault01CacheNodeUnreachable — the cache disappears mid-traffic.
//
// The claim is the one the whole design rests on: losing the cache costs hit rate and nothing else.
// Reads keep being served, from the database, with correct values. A cache that takes the
// application down with it is worse than no cache.
func TestFault01CacheNodeUnreachable(t *testing.T) {
	ctx := context.Background()
	cluster := startProxied(ctx, t, harness.CacheOptions{SynchronousInvalidation: true})
	c := cluster.Client(t, cluster.Addrs[0])

	const key = "entities:8100001"
	want := []byte("value-before-the-fault")
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: want},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Warm the entry, so the fault removes something that was actually being used.
	if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("warming Get: %v", err)
	}

	restore := disableProxy(ctx, t, "cache")
	defer restore()

	got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get with the cache unreachable must still be served from the database: %v", err)
	}
	if string(got.GetRecord().GetPayload()) != string(want) {
		t.Fatalf("payload = %q, want %q — a degraded cache must not change what a read returns",
			got.GetRecord().GetPayload(), want)
	}

	// Non-vacuity: if the cache were still reachable, this read would have been a hit and nothing
	// about the test would be about a fault.
	errs := cluster.CacheOpsForTest("get", "error")
	if errs == 0 {
		t.Fatal("no cache errors recorded — the proxy was not actually disabled, so this test proved nothing")
	}

	stats := cluster.Cache.BreakerStats()
	record(t, faultRecord{
		Number:      1,
		Title:       "Cache node unreachable mid-traffic",
		Injection:   "Toxiproxy: `POST /proxies/cache {\"enabled\": false}`",
		Claim:       "Reads continue, served from the database, returning the same value. Losing every entry is a correctness non-event.",
		Fired:       fmt.Sprintf("`cachet_cache_operations_total{op=\"get\",result=\"error\"}` = %d", errs),
		Observed:    "Every read succeeded with the pre-fault payload; no request failed.",
		Explanation: "`cachetctl health` reports the node as failing: " + breakerLine(stats),
	})
}

// TestFault03InvalidationSurvivesACachePartition — the backstop, actually back-stopping.
//
// This fault found a real defect. Under a cache partition the write path's invalidation fails, and
// the CDC tailer's invalidation failed too — it logged the error, dropped the event, and advanced
// its checkpoint past it. Both paths lost the same write, so the stale entry survived until its TTL
// with nothing reporting it. A backstop that one cache blip defeats is not a backstop.
//
// The tailer now retries, and freezes its checkpoint if the retries run out so a restart replays
// from before the lost event. See internal/cdc: the unit tests for both rules fail if either is
// removed.
func TestFault03InvalidationSurvivesACachePartition(t *testing.T) {
	ctx := context.Background()
	cluster := startProxied(ctx, t, harness.CacheOptions{SynchronousInvalidation: true})
	startTailers(t, cluster, 30*time.Second)
	c := cluster.Client(t, cluster.Addrs[0])

	const key = "entities:8100003"
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("warming Get: %v", err)
	}

	// The write lands while the cache cannot be reached, so neither invalidation path can deliver.
	restore := disableProxy(ctx, t, "cache")
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v2")},
	}); err != nil {
		t.Fatalf("Put v2 during the outage: %v", err)
	}
	tombstoneErrs := cluster.CacheOpsForTest("tombstone", "error")

	// Hold the partition open long enough that the tailer has certainly reached the event and
	// failed on it, rather than arriving after the cache was already back.
	time.Sleep(2 * time.Second)
	restore()

	if tombstoneErrs == 0 {
		t.Fatal("no tombstone errors recorded — the write did not encounter the outage, so this test proved nothing")
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("Get after the outage: %v", err)
		}
		if p := string(got.GetRecord().GetPayload()); p == "v2" {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("read still returns %q after the cache recovered: a write whose invalidation "+
				"could not be delivered left a stale entry readable", p)
		}
		time.Sleep(200 * time.Millisecond)
	}

	record(t, faultRecord{
		Number:    3,
		Title:     "Cache partitioned across a write — neither invalidation path can deliver",
		Injection: "Toxiproxy: cache proxy disabled across the write and for 2s after it, then restored",
		Claim:     "The CDC backstop retries rather than dropping the event, so the entry converges once the cache returns. Reads and fills may be shed; invalidations may not.",
		Fired:     fmt.Sprintf("`cachet_cache_operations_total{op=\"tombstone\",result=\"error\"}` = %d — the write path's invalidation did fail", tombstoneErrs),
		Observed:  "After the partition healed, the read returned the post-write value. Before the fix it returned the pre-write value indefinitely.",
		Explanation: "`cachetctl checkpoints` — a tailer that could not deliver freezes its position rather than advancing past the loss, " +
			"so a checkpoint that has stopped moving while the binlog has not is the visible symptom.",
	})
}

// TestFault02CacheSlowNotDead — the case a latch handles badly.
//
// A node that is slow rather than down is the harder failure: tripping fully open sends every key
// it owns to the database at once, turning a partial degradation into a cliff. The breaker is
// proportional for that reason, and this asserts it sheds rather than latching — and that reads
// still return correct values throughout.
func TestFault02CacheSlowNotDead(t *testing.T) {
	ctx := context.Background()
	cluster := startProxied(ctx, t, harness.CacheOptions{SynchronousInvalidation: true})
	c := cluster.Client(t, cluster.Addrs[0])

	const key = "entities:8100002"
	want := []byte("slow-but-correct")
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: want},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Far beyond any cache operation's patience, so the engine sees failures rather than slowness.
	remove := addToxic(ctx, t, "cache", "molasses", "latency", map[string]any{"latency": 3000, "jitter": 0})
	defer remove()

	const reads = 40
	for i := 0; i < reads; i++ {
		got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("read %d failed while the cache was slow: %v", i, err)
		}
		if string(got.GetRecord().GetPayload()) != string(want) {
			t.Fatalf("read %d returned %q, want %q", i, got.GetRecord().GetPayload(), want)
		}
	}

	errs := cluster.CacheOpsForTest("get", "error")
	if errs == 0 {
		t.Fatal("no cache errors recorded — the latency toxic did not take effect, so this test proved nothing")
	}

	// Proportional, not a latch: the breaker must be shedding, but not everything, or a partial
	// degradation has been converted into a total one.
	stats := cluster.Cache.BreakerStats()
	var shedding bool
	for _, s := range stats {
		if s.ShedRate > 0 {
			shedding = true
		}
		if s.ShedRate >= 1 {
			t.Fatalf("breaker shed rate reached %v — a full latch sends every key this node "+
				"owns to the database at once, which is the cliff the proportional design exists to avoid",
				s.ShedRate)
		}
	}
	if !shedding {
		t.Fatalf("no node is shedding despite %d cache errors; breaker state: %s", errs, breakerLine(stats))
	}

	record(t, faultRecord{
		Number:      2,
		Title:       "Cache node slow, not dead",
		Injection:   "Toxiproxy: 3000ms downstream latency toxic on the cache proxy",
		Claim:       "The breaker sheds proportionally rather than latching open, and reads stay correct throughout.",
		Fired:       fmt.Sprintf("`cachet_cache_operations_total{op=\"get\",result=\"error\"}` = %d over %d reads", errs, reads),
		Observed:    fmt.Sprintf("All %d reads returned the correct payload; shed probability stayed strictly between 0 and 1.", reads),
		Explanation: "`cachetctl health` prints per-node breaker state; during the fault: " + breakerLine(stats),
	})
}
