//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// TestCacheServesRepeatReads is the basic Phase 1 claim: a second read of the same key is served
// from the cache rather than from the database.
func TestCacheServesRepeatReads(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_400_001)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("cached")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The first read fills; a fresh session is used so the writer's own watermark does not force a
	// re-read, which is exactly the behaviour the next test relies on.
	first, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if first.GetMeta().GetCacheHit() {
		t.Error("the first read reported a cache hit before anything had filled the entry")
	}

	second, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if !second.GetMeta().GetCacheHit() {
		t.Error("the second read was not served from the cache")
	}
	if string(second.GetRecord().GetPayload()) != "cached" {
		t.Errorf("cached payload = %q, want \"cached\"", second.GetRecord().GetPayload())
	}
}

// TestStrongNeverReadsTheCache guards the one level that must not be cacheable.
func TestStrongNeverReadsTheCache(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_400_002)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("warm Get: %v", err)
		}
	}

	resp, err := c.Get(ctx, &cachetv1.GetRequest{
		Key:   key,
		Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG,
	})
	if err != nil {
		t.Fatalf("strong Get: %v", err)
	}
	if resp.GetMeta().GetCacheHit() {
		t.Error("a STRONG read was served from the cache")
	}
}

// TestReadOwnWritesHoldsWithNoInvalidation is the Phase 1 result that matters most.
//
// Phase 1 has NO invalidation — writes do not touch the cache at all. Read-own-writes nevertheless
// holds, because the session watermark rejects any entry filled from a database state older than
// the session's own write (CONSISTENCY.md §3.2).
//
// That is not luck. It is the payoff of watermarking on the FILL version rather than on the row
// version, decided in T2 before any of this code existed.
func TestReadOwnWritesHoldsWithNoInvalidation(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_400_003)

	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	// Warm the cache with v1.
	for i := 0; i < 3; i++ {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("warm Get: %v", err)
		}
	}

	put, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v2")},
	})
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	// The cache still holds v1 and nothing invalidated it. Carrying the write's session token must
	// still produce v2.
	got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key, Session: put.GetSession()})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if payload := string(got.GetRecord().GetPayload()); payload != "v2" {
		t.Errorf("read own write returned %q, want \"v2\" — the watermark did not reject the stale entry", payload)
	}
	if got.GetMeta().GetCacheHit() {
		t.Error("the stale entry was served as a cache hit to the session that had just overwritten it")
	}
}

// TestOtherSessionsSeeStaleData is the Phase 1 result that is deliberately BAD.
//
// It is written as an assertion rather than left as a known weakness because a documented
// limitation nobody has measured is just a rumour. Phase 2 replaces this test's expectation with
// its opposite, and the diff between them is the evidence that exact invalidation works.
//
// Note that this is not a violation of the model: CONSISTENCY.md §3.2 states plainly that SESSION
// does not promise read-OTHERS-writes. What Phase 1 lacks is any bound on how long that takes
// beyond the entry TTL.
func TestOtherSessionsSeeStaleData(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_400_004)

	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("warm Get: %v", err)
		}
	}

	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v2")},
	}); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	// A DIFFERENT session — no watermark — reads the same key.
	got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if payload := string(got.GetRecord().GetPayload()); payload != "v1" {
		t.Errorf("expected the KNOWN-BAD Phase 1 behaviour (stale \"v1\"), got %q.\n"+
			"If this now returns v2, invalidation has landed and this test should be replaced by "+
			"its Phase 2 opposite rather than deleted.", payload)
	}
	if !got.GetMeta().GetCacheHit() {
		t.Error("the stale value did not come from the cache, so this test is not measuring what it claims")
	}
	t.Log("PHASE 1 KNOWN LIMITATION: a committed write is invisible to other sessions until the TTL expires")
}

// TestBoundedRejectsAnEntryOlderThanItsWindow shows that BOUNDED is enforced end to end, which is
// the only bound Phase 1 offers other sessions.
func TestBoundedRejectsAnEntryOlderThanItsWindow(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_400_005)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("warm Get: %v", err)
		}
	}

	// A generous bound accepts the warm entry.
	fresh, err := c.Get(ctx, &cachetv1.GetRequest{
		Key:            key,
		Level:          cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED,
		StalenessBound: durationProto(time.Hour),
	})
	if err != nil {
		t.Fatalf("BOUNDED(1h) Get: %v", err)
	}
	if !fresh.GetMeta().GetCacheHit() {
		t.Error("BOUNDED(1h) did not accept a freshly filled entry")
	}

	time.Sleep(1200 * time.Millisecond)

	// A bound tighter than the entry's age must reject it and read through.
	stale, err := c.Get(ctx, &cachetv1.GetRequest{
		Key:            key,
		Level:          cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED,
		StalenessBound: durationProto(500 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("BOUNDED(500ms) Get: %v", err)
	}
	if stale.GetMeta().GetCacheHit() {
		t.Error("BOUNDED(500ms) served an entry older than its own window")
	}
}
