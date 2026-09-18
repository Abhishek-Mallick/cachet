//go:build chaos

package chaos_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// TestFault04CacheEvictsUnderMemoryPressure — the cache throws entries away without being asked.
//
// Eviction is the one fault the cache inflicts on itself, and the only acceptable behaviour is that
// it is INDISTINGUISHABLE FROM A MISS. An evicted entry costs a database read; it must never cost
// correctness. The risk worth injecting for is subtler than "the row disappeared": a design that
// kept any per-key state outside the entry — a version, a watermark, a tombstone marker — would
// have that state evicted independently, and could then answer from a half-present key.
//
// maxmemory is set and restored around the test. The stack is shared, so leaving it clamped would
// make every later run fail for a reason nobody would look for.
func TestFault04CacheEvictsUnderMemoryPressure(t *testing.T) {
	ctx := context.Background()
	cluster := startProxied(ctx, t, harness.CacheOptions{SynchronousInvalidation: true})
	c := cluster.Client(t, cluster.Addrs[0])

	// A direct connection, because setting maxmemory is an operator action rather than something
	// the cache client should ever be able to do.
	rdb := redis.NewClient(&redis.Options{Addr: harness.DefaultCacheAddr})
	// Registered BEFORE the restore below, because cleanups run last-in-first-out: the connection
	// has to outlive the restore that needs it. A `defer` here would close it first and leave the
	// shared stack clamped for every later run.
	t.Cleanup(func() { _ = rdb.Close() })

	origMaxMemory := configGet(t, rdb, "maxmemory")
	origPolicy := configGet(t, rdb, "maxmemory-policy")
	var restored bool
	restore := func() {
		if restored {
			return
		}
		restored = true
		configSet(t, rdb, "maxmemory", origMaxMemory)
		configSet(t, rdb, "maxmemory-policy", origPolicy)
	}
	// Belt and braces: the body restores as soon as the clamp has done its work, and this catches
	// the paths where the body fails before reaching that point.
	t.Cleanup(restore)

	const key = "entities:8400004"
	want := []byte("survives-eviction")
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: want},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("warming Get: %v", err)
	}
	if _, found, err := cluster.Cache.Get(ctx, key); err != nil || !found {
		t.Fatalf("the key was not cached before the fault: found=%v err=%v", found, err)
	}

	evictedBefore := evictedKeys(t, rdb)

	// The fault: clamp memory hard and push traffic through until the cache has to throw things
	// away. allkeys-lru so anything is eligible, including the key under test.
	configSet(t, rdb, "maxmemory-policy", "allkeys-lru")
	used := usedMemory(t, rdb)
	configSet(t, rdb, "maxmemory", fmt.Sprintf("%d", used+256*1024))

	for i := 0; i < 4000; i++ {
		k := "entities:" + itoa(8_410_000+i)
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: k, Record: &cachetv1.Record{TenantId: 1, Payload: make([]byte, 1024)},
		}); err != nil {
			t.Fatalf("filler Put %d: %v", i, err)
		}
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: k}); err != nil {
			t.Fatalf("filler Get %d: %v", i, err)
		}
		if i%500 == 0 && evictedKeys(t, rdb) > evictedBefore {
			break
		}
	}

	evicted := evictedKeys(t, rdb) - evictedBefore
	restore()

	// Non-vacuity: if nothing was evicted, the memory clamp did not bite and the read below proves
	// nothing about eviction.
	if evicted == 0 {
		t.Fatal("the cache evicted nothing despite the memory clamp — no fault was injected")
	}

	// The claim. Whether or not this particular key survived, the answer must be correct.
	got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("read after eviction: %v", err)
	}
	if string(got.GetRecord().GetPayload()) != string(want) {
		t.Fatalf("read after eviction returned %q, want %q — an evicted entry changed an answer "+
			"rather than only costing a database read", got.GetRecord().GetPayload(), want)
	}

	// And a write-then-read across the evicted state must still be correct: read-own-writes cannot
	// depend on the cache having kept anything.
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("after-eviction")},
	}); err != nil {
		t.Fatalf("Put after eviction: %v", err)
	}
	got, err = c.Get(ctx, &cachetv1.GetRequest{
		Key: key, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
	})
	if err != nil {
		t.Fatalf("SESSION read after eviction: %v", err)
	}
	if p := string(got.GetRecord().GetPayload()); p != "after-eviction" {
		t.Fatalf("read-own-writes broke across eviction: got %q", p)
	}

	record(t, faultRecord{
		Number:      4,
		Title:       "Cache evicts under memory pressure",
		Injection:   "`CONFIG SET maxmemory-policy allkeys-lru` and maxmemory clamped to 256KB above current usage, then traffic pushed through until keys are evicted",
		Claim:       "Eviction is indistinguishable from a miss: it costs a database read and never an answer. Read-own-writes holds across it.",
		Fired:       fmt.Sprintf("`evicted_keys` rose by %d during the clamp", evicted),
		Observed:    "The read returned the correct value after eviction, and a write-then-SESSION-read still returned the caller's own write.",
		Explanation: "`cachetctl inspect " + key + "` shows whether the entry is present and what fill version it holds; an evicted key reports as absent, which is the same thing the engine treats as a miss.",
	})
}

func configGet(t *testing.T, rdb *redis.Client, param string) string {
	t.Helper()

	vals, err := rdb.ConfigGet(context.Background(), param).Result()
	if err != nil {
		t.Fatalf("CONFIG GET %s: %v", param, err)
	}
	return vals[param]
}

func configSet(t *testing.T, rdb *redis.Client, param, value string) {
	t.Helper()

	if err := rdb.ConfigSet(context.Background(), param, value).Err(); err != nil {
		t.Fatalf("CONFIG SET %s %s: %v", param, value, err)
	}
}

func usedMemory(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	return infoField(t, rdb, "memory", "used_memory")
}

func evictedKeys(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	return infoField(t, rdb, "stats", "evicted_keys")
}

func infoField(t *testing.T, rdb *redis.Client, section, field string) int64 {
	t.Helper()

	info, err := rdb.Info(context.Background(), section).Result()
	if err != nil {
		t.Fatalf("INFO %s: %v", section, err)
	}
	for _, line := range strings.Split(info, "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok || name != field {
			continue
		}
		var n int64
		if _, err := fmt.Sscanf(value, "%d", &n); err != nil {
			t.Fatalf("parse %s from INFO %s: %v", field, section, err)
		}
		return n
	}
	t.Fatalf("%s not found in INFO %s", field, section)
	return 0
}
