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

// TestReadOwnWritesHoldsOnTheWatermarkAlone runs with synchronous invalidation DISABLED, so nothing
// removes the stale entry. Read-own-writes must still hold.
//
// The distinction matters. With invalidation on, this scenario would pass even if the watermark
// check were broken — the tombstone alone would carry it — and the suite would be proving nothing
// about the mechanism it claims to rely on. Turning invalidation off isolates the watermark.
//
// It holds because the watermark is compared against the entry's FILL version rather than the row
// version: an entry filled from a database state older than this session's own write is rejected
// whether or not anything invalidated it. That choice was made in T2, on paper, before any of this
// code existed (CONSISTENCY.md §3.2).
func TestReadOwnWritesHoldsOnTheWatermarkAlone(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCachedWith(ctx, t,
		harness.CacheOptions{TTL: time.Hour, SynchronousInvalidation: false},
		"tcp://127.0.0.1:0")
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
	// still produce v2, on the strength of the watermark alone.
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

// TestOtherSessionsSeeAWriteImmediately is the Phase 2 result, and it replaces the Phase 1 test
// that asserted the exact opposite.
//
// In Phase 1 this same scenario returned the stale value, bounded only by the entry TTL — measured
// at 10.10s against a 10s TTL, and up to four hours at the production default. The Phase 1 test
// asserted that bad behaviour deliberately, so that this diff would be evidence rather than a
// claim.
//
// What changed: the write path now tombstones the entry after the commit and before the ack. By the
// time the writer holds its response, the stale entry is already invalidated at that version — so
// a completely unrelated session reads the new value on its very next request.
func TestOtherSessionsSeeAWriteImmediately(t *testing.T) {
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

	// A DIFFERENT session — no watermark, nothing to protect it — reads the same key.
	got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if payload := string(got.GetRecord().GetPayload()); payload != "v2" {
		t.Errorf("another session read %q after a committed write of \"v2\"; "+
			"the entry was not invalidated before the ack", payload)
	}
}

// TestNegativeEntriesAreServedFromCache shows that "this row does not exist" is a cacheable answer.
func TestNegativeEntriesAreServedFromCache(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_450_001)

	first, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if first.GetFound() || first.GetMeta().GetCacheHit() {
		t.Fatalf("first Get on an absent row: found=%v hit=%v, want false/false",
			first.GetFound(), first.GetMeta().GetCacheHit())
	}

	// Without negative caching, every lookup of a missing row is a database query — so a workload
	// probing for absent keys bypasses the cache entirely and the origin sees all of it.
	second, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if second.GetFound() {
		t.Error("a row that does not exist was reported as found")
	}
	if !second.GetMeta().GetCacheHit() {
		t.Error("the absence was not cached; every lookup of a missing row still reaches the database")
	}
}

// TestReadOwnInsertOverANegativeEntry is the failure negative caching would introduce if the insert
// did not invalidate it: the writer would keep being told their own new row does not exist.
func TestReadOwnInsertOverANegativeEntry(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_450_002)

	// Cache the absence.
	for i := 0; i < 2; i++ {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("warm Get: %v", err)
		}
	}

	put, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("inserted")},
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key, Session: put.GetSession()})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("the inserted row was reported as missing; the negative entry outlived the insert")
	}
	if string(got.GetRecord().GetPayload()) != "inserted" {
		t.Errorf("payload = %q, want \"inserted\"", got.GetRecord().GetPayload())
	}
}

// TestDeleteIsVisibleToOtherSessions covers the other direction: a removed row must stop being
// served, not merely stop being updated.
func TestDeleteIsVisibleToOtherSessions(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCached(ctx, t, time.Hour, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_450_003)

	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("doomed")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("warm Get: %v", err)
		}
	}

	if _, err := c.Delete(ctx, &cachetv1.DeleteRequest{Key: key}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetFound() {
		t.Error("a deleted row was still served from the cache to another session")
	}
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
