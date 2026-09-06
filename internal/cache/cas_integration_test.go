//go:build integration

package cache_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
)

// The tests in this file are the compare-and-set invariant from CONSISTENCY.md §2, executable:
//
//	Every mutation of a cache entry is a compare-and-set. A lower version never overwrites a
//	higher one.
//
// Everything else in Cachet reduces to this. If these tests pass while the invariant is broken,
// they are worthless — so each one names the concrete failure it prevents.

func fill(ctx context.Context, t *testing.T, c *cache.Client, key string, rv, fv uint64, payload string) bool {
	t.Helper()
	applied, err := c.Fill(ctx, key, cache.Entry{RowVersion: rv, FillVersion: fv, Payload: []byte(payload)})
	if err != nil {
		t.Fatalf("Fill: %v", err)
	}
	return applied
}

func tombstone(ctx context.Context, t *testing.T, c *cache.Client, key string, v uint64) bool {
	t.Helper()
	applied, err := c.Tombstone(ctx, key, v)
	if err != nil {
		t.Fatalf("Tombstone: %v", err)
	}
	return applied
}

func mustGet(ctx context.Context, t *testing.T, c *cache.Client, key string) (cache.Entry, bool) {
	t.Helper()
	e, hit, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return e, hit
}

func TestFillAppliesToAnEmptyKey(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	if !fill(ctx, t, c, "cas:empty", 100, 100, "v1") {
		t.Fatal("the first fill of an empty key was rejected")
	}
	e, hit := mustGet(ctx, t, c, "cas:empty")
	if !hit || string(e.Payload) != "v1" {
		t.Errorf("Get = %q hit=%v, want \"v1\" hit=true", e.Payload, hit)
	}
}

func TestAHigherRowVersionWins(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	fill(ctx, t, c, "cas:higher", 100, 100, "v1")
	if !fill(ctx, t, c, "cas:higher", 200, 200, "v2") {
		t.Fatal("a newer fill was rejected")
	}
	if e, _ := mustGet(ctx, t, c, "cas:higher"); string(e.Payload) != "v2" {
		t.Errorf("payload = %q, want \"v2\"", e.Payload)
	}
}

func TestALowerRowVersionIsRejected(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	fill(ctx, t, c, "cas:lower", 200, 200, "v2")

	// The failure this prevents: a read that started before a write finishes after it, and refills
	// the value the write replaced. Without the version compare the cache would serve the old row
	// indefinitely, and nothing would record that it had happened.
	if fill(ctx, t, c, "cas:lower", 100, 100, "v1") {
		t.Error("a stale fill overwrote a newer value")
	}
	if e, _ := mustGet(ctx, t, c, "cas:lower"); string(e.Payload) != "v2" {
		t.Errorf("payload = %q, want the newer \"v2\"", e.Payload)
	}
}

func TestSameRowVersionWithAFresherSnapshotWins(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	fill(ctx, t, c, "cas:refresh", 100, 100, "v1")

	// The row has not changed, but the entry has been re-read from a newer database state. Updating
	// the fill version is what lets a re-read refresh an entry's freshness — the entire reason fill
	// version is tracked separately from row version.
	if !fill(ctx, t, c, "cas:refresh", 100, 500, "v1") {
		t.Fatal("a fresher snapshot of an unchanged row was rejected")
	}
	if e, _ := mustGet(ctx, t, c, "cas:refresh"); e.FillVersion != 500 {
		t.Errorf("fill version = %d, want 500", e.FillVersion)
	}
}

func TestSameRowVersionWithAnOlderSnapshotIsRejected(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	fill(ctx, t, c, "cas:stale-snapshot", 100, 500, "v1")

	// Moving the fill version BACKWARDS would make an entry look older than it is, which costs hit
	// rate — or, worse, could make an entry appear to satisfy a session watermark it does not.
	if fill(ctx, t, c, "cas:stale-snapshot", 100, 100, "v1") {
		t.Error("an older snapshot of the same row version was applied")
	}
	if e, _ := mustGet(ctx, t, c, "cas:stale-snapshot"); e.FillVersion != 500 {
		t.Errorf("fill version = %d, want it to stay at 500", e.FillVersion)
	}
}

func TestTombstoneRemovesTheValue(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	fill(ctx, t, c, "cas:tomb", 100, 100, "v1")
	if !tombstone(ctx, t, c, "cas:tomb", 200) {
		t.Fatal("a newer tombstone was rejected")
	}
	if _, hit := mustGet(ctx, t, c, "cas:tomb"); hit {
		t.Error("a tombstoned entry was still served")
	}
}

func TestAnOlderTombstoneIsRejected(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	fill(ctx, t, c, "cas:old-tomb", 200, 200, "v2")

	// CDC replay after a restart re-delivers old events. Applying them would invalidate current
	// data, turning a tailer restart into a cache-wide miss storm.
	if tombstone(ctx, t, c, "cas:old-tomb", 100) {
		t.Error("a tombstone older than the cached value was applied")
	}
	if _, hit := mustGet(ctx, t, c, "cas:old-tomb"); !hit {
		t.Error("an old tombstone invalidated current data")
	}
}

func TestAFillOlderThanATombstoneIsRejected(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	tombstone(ctx, t, c, "cas:resurrect", 200)

	// The resurrection race, and the reason invalidation writes a MARKER rather than deleting the
	// key. A reader that started before the write lands after it; with a plain delete there would be
	// nothing left to reject its fill, and the deleted value would come back to life.
	if fill(ctx, t, c, "cas:resurrect", 100, 100, "v1") {
		t.Error("a fill older than the tombstone resurrected a deleted value")
	}
	if _, hit := mustGet(ctx, t, c, "cas:resurrect"); hit {
		t.Error("the entry was served after being resurrected")
	}
}

func TestAFillNewerThanATombstoneApplies(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	tombstone(ctx, t, c, "cas:refill", 200)

	// A tombstone must not poison the key forever; the next genuine read has to be able to fill it.
	if !fill(ctx, t, c, "cas:refill", 300, 300, "v3") {
		t.Fatal("a fill newer than the tombstone was rejected")
	}
	if e, hit := mustGet(ctx, t, c, "cas:refill"); !hit || string(e.Payload) != "v3" {
		t.Errorf("Get = %q hit=%v, want \"v3\" hit=true", e.Payload, hit)
	}
}

func TestDuplicateTombstonesAreIdempotent(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	fill(ctx, t, c, "cas:dup", 100, 100, "v1")

	if !tombstone(ctx, t, c, "cas:dup", 200) {
		t.Fatal("the first tombstone was rejected")
	}
	// Replaying the binlog re-delivers the same event. Idempotency is what lets the CDC tailer get
	// away with at-least-once delivery instead of needing exactly-once, which nothing can provide.
	for i := 0; i < 5; i++ {
		if tombstone(ctx, t, c, "cas:dup", 200) {
			t.Fatalf("replay %d of the same tombstone was applied again", i+1)
		}
	}
	if _, hit := mustGet(ctx, t, c, "cas:dup"); hit {
		t.Error("the entry reappeared after replays")
	}
}

func TestOutOfOrderInvalidationConverges(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	// The write path and the CDC tailer both invalidate, and neither can be ordered against the
	// other. Whatever order they arrive in, the entry must end up in the same state — which is what
	// makes the tailer a backstop rather than a second source of truth.
	// Order A: fill(300) then tombstone(200)
	fill(ctx, t, c, "cas:order-a", 300, 300, "v3")
	tombstone(ctx, t, c, "cas:order-a", 200)
	_, hitA := mustGet(ctx, t, c, "cas:order-a")

	// Order B: tombstone(200) then fill(300)
	tombstone(ctx, t, c, "cas:order-b", 200)
	fill(ctx, t, c, "cas:order-b", 300, 300, "v3")
	_, hitB := mustGet(ctx, t, c, "cas:order-b")

	if hitA != hitB {
		t.Errorf("delivery order changed the outcome: fill-first hit=%v, tombstone-first hit=%v", hitA, hitB)
	}
	if !hitA {
		t.Error("the newer value did not survive an older invalidation")
	}
}

func TestNegativeEntriesRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	applied, err := c.Fill(ctx, "cas:negative", cache.Entry{RowVersion: 100, FillVersion: 100, Negative: true})
	if err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if !applied {
		t.Fatal("a negative fill was rejected")
	}

	// "This row does not exist" is a cacheable fact. Without it, every lookup of an absent row is a
	// database query, and a workload probing for missing keys bypasses the cache entirely.
	e, hit := mustGet(ctx, t, c, "cas:negative")
	if !hit {
		t.Fatal("the negative entry was not returned")
	}
	if !e.Negative {
		t.Error("the entry came back without its negative flag, so it would be served as a real row")
	}
}

func TestNegativeEntriesObeyTheSameCASRules(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	if _, err := c.Fill(ctx, "cas:neg-cas", cache.Entry{RowVersion: 100, FillVersion: 100, Negative: true}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	// An insert must invalidate the negative entry, or read-own-inserts is impossible: the writer
	// would keep being told their own new row does not exist.
	if !tombstone(ctx, t, c, "cas:neg-cas", 200) {
		t.Fatal("an insert's tombstone did not clear the negative entry")
	}
	if _, hit := mustGet(ctx, t, c, "cas:neg-cas"); hit {
		t.Error("the negative entry survived the insert that contradicted it")
	}
}

func TestConcurrentFillsLeaveTheHighestVersion(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	const key = "cas:concurrent"
	const versions = 200

	var wg sync.WaitGroup
	for i := 1; i <= versions; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := uint64(i)
			if _, err := c.Fill(ctx, key, cache.Entry{
				RowVersion: v, FillVersion: v, Payload: []byte(fmt.Sprintf("v%d", i)),
			}); err != nil {
				t.Errorf("Fill: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Concurrency is where a compare-and-set either holds or is decorative. Whatever order the
	// fills interleave in, the highest version must be the one left standing.
	e, hit := mustGet(ctx, t, c, key)
	if !hit {
		t.Fatal("the key is empty after concurrent fills")
	}
	if e.RowVersion != versions {
		t.Errorf("row version = %d after %d concurrent fills, want %d", e.RowVersion, versions, versions)
	}
}

func TestVersionsNearTheDoublePrecisionLimitCompareExactly(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	// 2^53+1 and 2^53+2 are indistinguishable as IEEE doubles. Real HLC versions live well above
	// this range — physical milliseconds are already ~1.7e12, shifted left 16 bits — so if the Lua
	// scripts ever passed a version through tonumber(), adjacent versions would compare EQUAL and a
	// stale fill would win. This test fails the moment that happens.
	const lo = uint64(1)<<53 + 1
	const hi = uint64(1)<<53 + 2

	fill(ctx, t, c, "cas:precision", hi, hi, "high")
	if fill(ctx, t, c, "cas:precision", lo, lo, "low") {
		t.Error("a version one below the current one was treated as newer; versions are being rounded")
	}
	if e, _ := mustGet(ctx, t, c, "cas:precision"); string(e.Payload) != "high" {
		t.Errorf("payload = %q, want \"high\"", e.Payload)
	}
}
