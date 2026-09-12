//go:build e2e

package conformance_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// Each function here is one row of the matrix. They return whether the property HELD rather than
// calling t.Error, because the same scenario is a promise at one level and an explicit non-promise
// at another — the matrix decides what to do with the answer, not the scenario.
//
// t.Fatal is still used for things that are never part of the property under test: a transport
// error, a malformed response, a broken fixture. A cell must not report "the guarantee failed" when
// what actually happened is that the harness did.

func durationProto(d time.Duration) *durationpb.Duration { return durationpb.New(d) }

// readOwnPointWrite: a session that wrote k must see that write or a later one.
func readOwnPointWrite(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	put(t, e, key, "v1", nil)
	// Warm the cache so the read has something stale to be fooled by. Without this the read would
	// miss and go to the database, and the cell would pass without ever exercising the guarantee.
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)

	w := put(t, e, key, "v2", nil)
	resp := get(t, e, key, lv, w.GetSession())

	return resp.GetFound() && string(resp.GetRecord().GetPayload()) == "v2"
}

// readOwnInsertOverNegative: an insert must be visible to its writer even when the key's absence was
// cached first. Without negative-entry invalidation the writer reads its own insert as "not found".
func readOwnInsertOverNegative(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	// Cache the absence.
	if resp := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil); resp.GetFound() {
		t.Fatalf("fixture: %s already exists", key)
	}
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)

	w := put(t, e, key, "inserted", nil)
	resp := get(t, e, key, lv, w.GetSession())

	return resp.GetFound() && string(resp.GetRecord().GetPayload()) == "inserted"
}

// readOwnDelete: the read must return "not found", never the old value.
func readOwnDelete(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	put(t, e, key, "doomed", nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)

	del, err := e.client.Delete(context.Background(), &cachetv1.DeleteRequest{Key: key})
	if err != nil {
		t.Fatalf("Delete %s: %v", key, err)
	}

	resp := get(t, e, key, lv, del.GetSession())
	return !resp.GetFound()
}

// readAnotherSessionImmediately: a DIFFERENT session, carrying no knowledge of the write, reads
// immediately after the writer's ack.
//
// SESSION explicitly does not promise this (CONSISTENCY.md §3.2). With synchronous invalidation on
// it happens to hold anyway, which is why the matrix records the observation instead of asserting
// either way — forbidding the system from being better than its contract would be as wrong as
// promising it.
func readAnotherSessionImmediately(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	put(t, e, key, "v1", nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)

	put(t, e, key, "v2", nil)

	// A fresh session: no token, so nothing but invalidation can make the new value visible.
	resp := get(t, e, key, lv, nil)
	return resp.GetFound() && string(resp.GetRecord().GetPayload()) == "v2"
}

// readAnotherSessionAfterP: the same, but after the propagation bound has elapsed.
//
// This one IS promised at every level, and it is the cell the naive cache cannot pass: with no
// invalidation the entry sits in the cache until its TTL, and waiting longer changes nothing.
func readAnotherSessionAfterP(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	put(t, e, key, "v1", nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)

	put(t, e, key, "v2", nil)

	// Poll for the propagation bound rather than sleeping it out, so a system that converges in a
	// millisecond is not charged two seconds of test time for it.
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp := get(t, e, key, lv, nil)
		if resp.GetFound() && string(resp.GetRecord().GetPayload()) == "v2" {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// monotonicReads: within one session, successive reads of a key never move backwards in version.
//
// Run against concurrent writers, because a monotonicity bug is a race and a quiet system will not
// show it.
func monotonicReads(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()
	put(t, e, key, "v0", nil)

	var (
		wg      sync.WaitGroup
		stop    = make(chan struct{})
		monoton = true
		mu      sync.Mutex
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			put(t, e, key, fmt.Sprintf("w%d", i), nil)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// One session, reading repeatedly and carrying its token forward. The watermark it accumulates
	// is what must stop it ever seeing an older version than one it has already been shown.
	var token *cachetv1.SessionToken
	var last uint64
	for i := 0; i < 40; i++ {
		resp := get(t, e, key, lv, token)
		token = resp.GetSession()

		if v := resp.GetMeta().GetRowVersion(); v < last {
			mu.Lock()
			monoton = false
			mu.Unlock()
			break
		} else if v > last {
			last = v
		}
		time.Sleep(5 * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	return monoton
}

// stalenessBounded: a BOUNDED(t) read must reflect every write committed at or before T−t.
//
// The entry is deliberately made older than the window, so a system that ignored the bound would
// serve it and fail here.
func stalenessBounded(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	put(t, e, key, "v1", nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)

	// A write that bypasses cache invalidation entirely, straight to the shard, so the cached entry
	// is genuinely stale rather than merely old.
	writeBehindTheCache(t, e, key, "v2")

	if lv.bound == 0 {
		// Not a BOUNDED read: whatever comes back, there is no bound to have honoured.
		resp := get(t, e, key, lv, nil)
		return resp.GetFound() && string(resp.GetRecord().GetPayload()) == "v2"
	}

	// Wait past the bound. After it, the entry's fill version is older than T−t and must be refused.
	time.Sleep(lv.bound + 500*time.Millisecond)

	resp := get(t, e, key, lv, nil)
	return resp.GetFound() && string(resp.GetRecord().GetPayload()) == "v2"
}

// survivesCacheFlush: losing every cache entry must be a correctness non-event.
func survivesCacheFlush(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	w := put(t, e, key, "v1", nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)

	// The cache is configured with no persistence precisely so this is survivable. If a flush could
	// lose a guarantee, the cache would be a source of truth, which it must never become.
	if err := e.cluster.Cache.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	resp := get(t, e, key, lv, w.GetSession())
	return resp.GetFound() && string(resp.GetRecord().GetPayload()) == "v1"
}

// survivesEngineFailover: the session guarantee must survive the engine that served the write.
//
// Engines are stateless with respect to sessions — the token is held by the CLIENT — and this is the
// cell that proves it. A server-side session map would pass every other test in this file and fail
// this one.
func survivesEngineFailover(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	w := put(t, e, key, "v1", nil)
	get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)

	// A second engine, sharing the cache and shards, that has never seen this session. The cache is
	// preserved rather than flushed: flushing would erase the state under test and let the new
	// engine pass by reading everything from the database.
	ctx := context.Background()
	other := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: e.cfg.synchronousInvalidation,
		PreserveCache:           true,
	}, "tcp://127.0.0.1:0")
	oc := other.Client(t, other.Addrs[0])

	req := &cachetv1.GetRequest{Key: key, Level: lv.level, Session: w.GetSession()}
	if lv.bound > 0 {
		req.StalenessBound = durationProto(lv.bound)
	}
	resp, err := oc.Get(ctx, req)
	if err != nil {
		t.Fatalf("Get on the failover engine: %v", err)
	}

	return resp.GetFound() && string(resp.GetRecord().GetPayload()) == "v1"
}

// crossKeySnapshot: two keys read in one BatchGet must NOT be guaranteed to reflect the same instant.
//
// Cachet caches rows, not transactions. This cell asserts the absence: if a snapshot ever started
// holding, it would be a promise nobody decided to make, and applications would begin relying on it
// long before anyone noticed it was accidental.
func crossKeySnapshot(t *testing.T, e *env, lv levelSpec) bool {
	a, b := e.key(), e.key()

	put(t, e, a, "0", nil)
	put(t, e, b, "0", nil)

	// A writer advancing both keys in lockstep. Under a real snapshot, every batch read would show
	// the two keys agreeing. Under Cachet they are N independent reads and will diverge.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			v := fmt.Sprintf("%d", i)
			put(t, e, a, v, nil)
			put(t, e, b, v, nil)
		}
	}()

	diverged := false
	for i := 0; i < 200 && !diverged; i++ {
		req := &cachetv1.BatchGetRequest{Keys: []string{a, b}, Level: lv.level}
		if lv.bound > 0 {
			req.StalenessBound = durationProto(lv.bound)
		}
		resp, err := e.client.BatchGet(context.Background(), req)
		if err != nil {
			t.Fatalf("BatchGet: %v", err)
		}
		ra, oka := resp.GetRecords()[a]
		rb, okb := resp.GetRecords()[b]
		if oka && okb && string(ra.GetPayload()) != string(rb.GetPayload()) {
			diverged = true
		}
	}

	close(stop)
	wg.Wait()

	// "held" means a snapshot appeared to hold, which is what the matrix marks ❌.
	return !diverged
}

// writeBehindTheCache commits straight to the shard, so no invalidation of any kind runs.
//
// It exists for the staleness cell, which needs an entry that is genuinely out of date rather than
// merely old. Going through the engine would tombstone the entry and there would be nothing stale
// left to bound.
func writeBehindTheCache(t *testing.T, e *env, key, payload string) {
	t.Helper()

	id := parseID(t, key)
	shardID, err := e.cluster.Router.ShardFor(key)
	if err != nil {
		t.Fatalf("route %s: %v", key, err)
	}
	shard, ok := e.cluster.Shards[shardID]
	if !ok {
		t.Fatalf("no shard %s in the cluster", shardID)
	}
	if _, err := shard.Put(context.Background(), storageRecord(id, payload)); err != nil {
		t.Fatalf("direct shard write: %v", err)
	}
}

// parseID extracts the numeric id from "entities:<id>".
func parseID(t *testing.T, key string) uint64 {
	t.Helper()

	var id uint64
	if _, err := fmt.Sscanf(key, "entities:%d", &id); err != nil {
		t.Fatalf("parse id from %q: %v", key, err)
	}
	return id
}

// storageRecord builds the row for a direct shard write. Version is left zero: the shard stamps it
// from its own HLC, which is the only clock allowed to issue one.
func storageRecord(id uint64, payload string) storage.Record {
	return storage.Record{ID: id, TenantID: 1, Status: 0, Payload: []byte(payload)}
}
