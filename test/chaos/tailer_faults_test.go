//go:build chaos

package chaos_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/cdc"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// oneTailer runs a single shard's tailer and returns a stop function plus its invalidation counter.
//
// Faults 6 and 7 need to kill and restart a tailer independently, which the all-shards helper
// cannot express: stopping all three would make "did it resume from its checkpoint" a question
// about three moving parts instead of one.
type tailerHandle struct {
	stop       func()
	invalidate *atomic.Int64
	checkpoint *cdc.FileCheckpoint
}

func oneTailer(t *testing.T, cluster *harness.Cluster, shard, addr string, serverID uint32, cpPath string) *tailerHandle {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	counter := &atomic.Int64{}
	cp := cdc.NewFileCheckpoint(cpPath)

	tailer, err := cdc.New(cdc.Options{
		ShardID:         shard,
		Addr:            addr,
		User:            "root",
		Password:        "cachet",
		Database:        "cachet",
		Table:           "entities",
		ServerID:        serverID,
		Cache:           cluster.Cache,
		Checkpoint:      cp,
		CheckpointEvery: 100 * time.Millisecond,
		OnInvalidate:    func(string, uint64, bool) { counter.Add(1) },
	})
	if err != nil {
		cancel()
		t.Fatalf("new tailer for %s: %v", shard, err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := tailer.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("tailer for %s stopped: %v", shard, err)
		}
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Error("tailer did not shut down")
			}
		})
	}
	t.Cleanup(stop)
	return &tailerHandle{stop: stop, invalidate: counter, checkpoint: cp}
}

// keysOn returns n keys that all route to the given shard.
func keysOn(t *testing.T, r *storage.Router, target storage.ShardID, base, n int) []string {
	t.Helper()

	var out []string
	for i := 0; i < 200_000 && len(out) < n; i++ {
		key := "entities:" + itoa(base+i)
		id, err := r.ShardFor(key)
		if err != nil {
			t.Fatalf("ShardFor: %v", err)
		}
		if id == target {
			out = append(out, key)
		}
	}
	if len(out) < n {
		t.Fatalf("found only %d keys on %s, want %d", len(out), target, n)
	}
	return out
}

// waitForCount blocks until the counter reaches want.
func waitForCount(t *testing.T, c *atomic.Int64, want int64, within time.Duration, what string) {
	t.Helper()

	deadline := time.Now().Add(within)
	for {
		if c.Load() >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: saw %d, want %d", what, c.Load(), want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFault06TailerKilledMidStream — the backstop is killed while writes are in flight.
//
// Synchronous invalidation is OFF here, so the tailer is the ONLY thing that can invalidate. That
// is what makes the fault meaningful: a write made while it is down leaves a genuinely stale entry,
// and the test proves the staleness exists before asserting it goes away. A tailer that resumed
// from the current binlog position instead of its checkpoint would skip those writes silently, and
// the entries would stay stale with nothing reporting it.
func TestFault06TailerKilledMidStream(t *testing.T) {
	ctx := context.Background()
	cluster := startProxied(ctx, t, harness.CacheOptions{SynchronousInvalidation: false})
	c := cluster.Client(t, cluster.Addrs[0])

	const target = storage.ShardID("shard0")
	cpPath := filepath.Join(t.TempDir(), "shard0.pos")
	tailer := oneTailer(t, cluster, string(target), "127.0.0.1:3316", 4711, cpPath)

	keys := keysOn(t, cluster.Router, target, 8_600_000, 4)

	for _, k := range keys {
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: k, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
		}); err != nil {
			t.Fatalf("Put v1 %s: %v", k, err)
		}
	}
	waitForCount(t, tailer.invalidate, int64(len(keys)), 60*time.Second, "tailer did not attach before the kill")

	// Warm every key, so the cache holds v1 and a lost invalidation has something to leave stale.
	for _, k := range keys {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{
			Key: k, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
		}); err != nil {
			t.Fatalf("warming Get %s: %v", k, err)
		}
	}

	time.Sleep(300 * time.Millisecond) // let the periodic saver persist the position
	before, ok, err := tailer.checkpoint.Load()
	if err != nil || !ok {
		t.Fatalf("checkpoint not written before the kill: ok=%v err=%v", ok, err)
	}

	// The fault: the tailer dies, and writes keep landing behind its back.
	tailer.stop()

	for _, k := range keys {
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: k, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v2")},
		}); err != nil {
			t.Fatalf("Put v2 %s during the outage: %v", k, err)
		}
	}

	// Non-vacuity, and the point of the fault: with nothing tailing and no synchronous
	// invalidation, those reads are now stale. If they are not, the rest of this test is theatre.
	stale := 0
	for _, k := range keys {
		got, err := c.Get(ctx, &cachetv1.GetRequest{
			Key: k, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
		})
		if err != nil {
			t.Fatalf("Get %s during the outage: %v", k, err)
		}
		if string(got.GetRecord().GetPayload()) == "v1" {
			stale++
		}
	}
	if stale == 0 {
		t.Fatal("no key was stale while the tailer was down — something else invalidated them, so " +
			"this test cannot show that the tailer recovered anything")
	}

	// Restart from the same checkpoint file, as a supervisor would.
	restarted := oneTailer(t, cluster, string(target), "127.0.0.1:3316", 4712, cpPath)

	deadline := time.Now().Add(60 * time.Second)
	for {
		converged := 0
		for _, k := range keys {
			got, err := c.Get(ctx, &cachetv1.GetRequest{
				Key: k, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
			})
			if err != nil {
				t.Fatalf("Get %s after restart: %v", k, err)
			}
			if string(got.GetRecord().GetPayload()) == "v2" {
				converged++
			}
		}
		if converged == len(keys) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d keys still stale after the tailer restarted: it resumed without "+
				"replaying the writes made while it was down", len(keys)-converged, len(keys))
		}
		time.Sleep(250 * time.Millisecond)
	}

	after, _, err := restarted.checkpoint.Load()
	if err != nil {
		t.Fatalf("checkpoint after restart: %v", err)
	}
	if after.Before(before) {
		t.Fatalf("checkpoint went backwards: %s then %s", before, after)
	}

	record(t, faultRecord{
		Number:    6,
		Title:     "Binlog tailer killed mid-stream",
		Injection: "The tailer's context is cancelled while writes continue, then a new tailer is started from the same checkpoint file. Synchronous invalidation is off, so the tailer is the only path that can invalidate.",
		Claim:     "A restart resumes from the checkpoint, so writes made while the tailer was down are invalidated rather than skipped.",
		Fired: fmt.Sprintf("Stopped at checkpoint `%s`; %d of %d keys were verifiably serving stale reads while nothing was tailing",
			before, stale, len(keys)),
		Observed: fmt.Sprintf("After restart all %d converged to the post-outage value, and the checkpoint advanced to `%s`.",
			len(keys), after),
		Explanation: "`cachetctl checkpoints` prints each tailer's position; one that resumes where it stopped is the evidence, and one frozen behind the binlog is the symptom of a delivery it could not make.",
	})
}

// TestFault07TailerRewound — the checkpoint is moved backwards under it.
//
// This is what an operator does by hand after an incident, and what a restored backup of the state
// directory does by accident. Replaying old events must be harmless: every invalidation is a
// versioned compare-and-set, so a tombstone carrying an OLD version cannot remove an entry filled
// from a NEWER one. Without that, a rewind would erase current cache state and, worse, could
// resurrect a stale read window.
func TestFault07TailerRewound(t *testing.T) {
	ctx := context.Background()
	cluster := startProxied(ctx, t, harness.CacheOptions{SynchronousInvalidation: false})
	c := cluster.Client(t, cluster.Addrs[0])

	const target = storage.ShardID("shard0")
	cpPath := filepath.Join(t.TempDir(), "shard0.pos")
	tailer := oneTailer(t, cluster, string(target), "127.0.0.1:3316", 4713, cpPath)

	key := keysOn(t, cluster.Router, target, 8_700_000, 1)[0]

	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	waitForCount(t, tailer.invalidate, 1, 60*time.Second, "tailer did not attach")
	time.Sleep(300 * time.Millisecond)

	rewindTo, ok, err := tailer.checkpoint.Load()
	if err != nil || !ok {
		t.Fatalf("no checkpoint to rewind to: ok=%v err=%v", ok, err)
	}

	// Move the row forward, and let the cache hold the CURRENT value.
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v2")},
	}); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	waitForCount(t, tailer.invalidate, 2, 60*time.Second, "tailer did not see the second write")
	tailer.stop()

	got, err := c.Get(ctx, &cachetv1.GetRequest{
		Key: key, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
	})
	if err != nil {
		t.Fatalf("Get to warm the current value: %v", err)
	}
	if p := string(got.GetRecord().GetPayload()); p != "v2" {
		t.Fatalf("cache warmed with %q, want v2", p)
	}

	// The fault: rewind the checkpoint to before the second write and restart.
	if err := tailer.checkpoint.Save(rewindTo); err != nil {
		t.Fatalf("rewind the checkpoint: %v", err)
	}
	replayed := oneTailer(t, cluster, string(target), "127.0.0.1:3316", 4714, cpPath)
	waitForCount(t, replayed.invalidate, 1, 60*time.Second, "the rewound tailer replayed nothing")

	// The assertion has to be about the ENTRY, not about what a read returns. A wrongly applied
	// tombstone would evict the entry and the next read would fetch v2 from the database anyway —
	// so "the read said v2" is satisfied whether the compare-and-set worked or not, and asserting
	// it would prove nothing. What must survive is the cached entry itself.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entry, found, err := cluster.Cache.Get(ctx, key)
		if err != nil {
			t.Fatalf("inspect the cached entry during replay: %v", err)
		}
		if !found {
			t.Fatalf("the rewound tailer evicted the cached entry for %s: an invalidation carrying "+
				"an older version was applied to an entry filled from a newer one", key)
		}
		if got := string(entry.Payload); got != "v2" {
			t.Fatalf("cached entry became %q during replay, want v2", got)
		}
		time.Sleep(200 * time.Millisecond)
	}

	record(t, faultRecord{
		Number:      7,
		Title:       "Tailer rewound to an old checkpoint",
		Injection:   fmt.Sprintf("The checkpoint file is rewritten to an earlier position (`%s`) and the tailer restarted", rewindTo),
		Claim:       "Replay is idempotent: an invalidation carrying an older version cannot act on an entry filled from a newer one.",
		Fired:       fmt.Sprintf("The rewound tailer re-processed %d event(s) it had already applied", replayed.invalidate.Load()),
		Observed:    "Reads returned the current value throughout the replay, never the pre-rewind one.",
		Explanation: "`cachetctl inspect " + key + "` shows the entry's fill version against the row version; a tombstone whose version is older is rejected by the compare-and-set rather than applied.",
	})
}
