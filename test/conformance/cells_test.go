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

// eventually is the warming read every cell takes before the one under test.
//
// EVENTUAL because warming must not itself carry a guarantee: a warming read at SESSION would
// advance the watermark and change what the cell then measures.
var eventually = levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}

// readOwnPointWrite: a session that wrote k must see that write or a later one.
func readOwnPointWrite(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	e.fx.Put(t, e, key, "v1", nil)
	// Warm the cache so the read has something stale to be fooled by. Without this the read would
	// miss and go to the database, and the cell would pass without ever exercising the guarantee.
	e.fx.Get(t, e, key, eventually, nil)
	e.fx.Get(t, e, key, eventually, nil)

	w := e.fx.Put(t, e, key, "v2", nil)
	resp := e.fx.Get(t, e, key, lv, w.Session)

	return resp.Found && resp.Payload == "v2"
}

// readOwnInsertOverNegative: an insert must be visible to its writer even when the key's absence was
// cached first. Without negative-entry invalidation the writer reads its own insert as "not found".
func readOwnInsertOverNegative(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	// Cache the absence.
	if resp := e.fx.Get(t, e, key, eventually, nil); resp.Found {
		t.Fatalf("fixture: %s already exists", key)
	}
	e.fx.Get(t, e, key, eventually, nil)

	w := e.fx.Put(t, e, key, "inserted", nil)
	resp := e.fx.Get(t, e, key, lv, w.Session)

	return resp.Found && resp.Payload == "inserted"
}

// readOwnDelete: the read must return "not found", never the old value.
func readOwnDelete(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	e.fx.Put(t, e, key, "doomed", nil)
	e.fx.Get(t, e, key, eventually, nil)
	e.fx.Get(t, e, key, eventually, nil)

	del := e.fx.Delete(t, e, key, nil)

	resp := e.fx.Get(t, e, key, lv, del.Session)
	return !resp.Found
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

	e.fx.Put(t, e, key, "v1", nil)
	e.fx.Get(t, e, key, eventually, nil)
	e.fx.Get(t, e, key, eventually, nil)

	e.fx.Put(t, e, key, "v2", nil)

	// A fresh session: no token, so nothing but invalidation can make the new value visible.
	resp := e.fx.Get(t, e, key, lv, nil)
	return resp.Found && resp.Payload == "v2"
}

// readAnotherSessionAfterP: the same, but after the propagation bound has elapsed.
//
// This one IS promised at every level, and it is the cell the naive cache cannot pass: with no
// invalidation the entry sits in the cache until its TTL, and waiting longer changes nothing.
func readAnotherSessionAfterP(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	e.fx.Put(t, e, key, "v1", nil)
	e.fx.Get(t, e, key, eventually, nil)
	e.fx.Get(t, e, key, eventually, nil)

	e.fx.Put(t, e, key, "v2", nil)

	// Poll for the propagation bound rather than sleeping it out, so a system that converges in a
	// millisecond is not charged two seconds of test time for it.
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp := e.fx.Get(t, e, key, lv, nil)
		if resp.Found && resp.Payload == "v2" {
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
	e.fx.Put(t, e, key, "v0", nil)

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
			e.fx.Put(t, e, key, fmt.Sprintf("w%d", i), nil)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// One session, reading repeatedly and carrying its token forward. The watermark it accumulates
	// is what must stop it ever seeing an older version than one it has already been shown.
	var token session
	var last uint64
	for i := 0; i < 40; i++ {
		resp := e.fx.Get(t, e, key, lv, token)
		token = resp.Session

		if v := resp.RowVersion; v < last {
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

	e.fx.Put(t, e, key, "v1", nil)
	e.fx.Get(t, e, key, eventually, nil)
	e.fx.Get(t, e, key, eventually, nil)

	// A write that bypasses cache invalidation entirely, straight to the shard, so the cached entry
	// is genuinely stale rather than merely old.
	e.fx.WriteBehindTheCache(t, e, key, "v2")

	if lv.bound == 0 {
		// Not a BOUNDED read: whatever comes back, there is no bound to have honoured.
		resp := e.fx.Get(t, e, key, lv, nil)
		return resp.Found && resp.Payload == "v2"
	}

	// Wait past the bound. After it, the entry's fill version is older than T−t and must be refused.
	time.Sleep(lv.bound + 500*time.Millisecond)

	resp := e.fx.Get(t, e, key, lv, nil)
	return resp.Found && resp.Payload == "v2"
}

// survivesCacheFlush: losing every cache entry must be a correctness non-event.
func survivesCacheFlush(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	w := e.fx.Put(t, e, key, "v1", nil)
	e.fx.Get(t, e, key, eventually, nil)

	// The cache is configured with no persistence precisely so this is survivable. If a flush could
	// lose a guarantee, the cache would be a source of truth, which it must never become.
	if err := e.cluster.Cache.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	resp := e.fx.Get(t, e, key, lv, w.Session)
	return resp.Found && resp.Payload == "v1"
}

// survivesEngineFailover: the session guarantee must survive the engine that served the write.
//
// Engines are stateless with respect to sessions — the token is held by the CLIENT — and this is the
// cell that proves it. A server-side session map would pass every other test in this file and fail
// this one.
func survivesEngineFailover(t *testing.T, e *env, lv levelSpec) bool {
	key := e.key()

	w := e.fx.Put(t, e, key, "v1", nil)
	e.fx.Get(t, e, key, eventually, nil)

	// A second engine, sharing the cache and shards, that has never seen this session. The cache is
	// preserved rather than flushed: flushing would erase the state under test and let the new
	// engine pass by reading everything from the database.
	ctx := context.Background()
	other := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: e.cfg.synchronousInvalidation,
		PreserveCache:           true,
		BothTables:              e.fx.BothTables(),
	}, "tcp://127.0.0.1:0")

	resp := e.fx.GetOn(t, other, key, lv, w.Session)
	return resp.Found && resp.Payload == "v1"
}

// crossKeySnapshot: two keys read in one BatchGet must NOT be guaranteed to reflect the same instant.
//
// Cachet caches rows, not transactions. This cell asserts the absence: if a snapshot ever started
// holding, it would be a promise nobody decided to make, and applications would begin relying on it
// long before anyone noticed it was accidental.
func crossKeySnapshot(t *testing.T, e *env, lv levelSpec) bool {
	a, b := e.key(), e.key()

	e.fx.Put(t, e, a, "0", nil)
	e.fx.Put(t, e, b, "0", nil)

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
			e.fx.Put(t, e, a, v, nil)
			e.fx.Put(t, e, b, v, nil)
		}
	}()

	diverged := false
	for i := 0; i < 200 && !diverged; i++ {
		got := e.fx.BatchPayloads(t, e, []string{a, b}, lv)
		ra, oka := got[a]
		rb, okb := got[b]
		if oka && okb && ra != rb {
			diverged = true
		}
	}

	close(stop)
	wg.Wait()

	// "held" means a snapshot appeared to hold, which is what the matrix marks ❌.
	return !diverged
}
