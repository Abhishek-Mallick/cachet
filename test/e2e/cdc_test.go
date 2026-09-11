//go:build e2e

package e2e_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/cdc"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// This file is the Phase 2 exit gate. It answers one question:
//
//	Can invalidation be exact and idempotent under CDC replay, with the tailer restarted mid-stream?
//
// Everything here writes DIRECTLY to MySQL, bypassing the engine, and every cluster runs with
// synchronous invalidation OFF. Both are deliberate. Out-of-band writes are precisely what CDC
// exists to catch — migrations, admin scripts, an engine that died between commit and invalidation —
// and leaving the write path on would let it do CDC's work while CDC took the credit.

// cdcShards maps a shard id to the DSN in test/env/compose.yml, and to a replication server id.
// Server ids must be unique across every replica and tailer attached to the same MySQL instance, or
// the two fight over the connection.
var cdcShards = []struct {
	id       string
	addr     string
	serverID uint32
}{
	{"shard0", "127.0.0.1:3316", 4101},
	{"shard1", "127.0.0.1:3317", 4102},
	{"shard2", "127.0.0.1:3318", 4103},
}

func openShardDB(t *testing.T, addr string) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("root:cachet@tcp(%s)/cachet?parseTime=true&interpolateParams=true", addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", addr, err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		// Fatal, never skipped. A gate that goes green when its environment is missing stops
		// protecting anything the first time someone forgets a step (test/README.md).
		t.Fatalf("%s is not reachable (%v); run `make env-up` first", addr, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// tailerFleet is one tailer per shard, sharing a checkpoint directory so a restart resumes exactly
// where the previous generation stopped.
type tailerFleet struct {
	cancel   context.CancelFunc
	done     chan struct{}
	applied  atomic.Int64
	rejected atomic.Int64
}

// startFleet runs a tailer for every shard against the cluster's real cache.
//
// The real cache, not a recorder: the invariant under test is a compare-and-set inside Lua, and a
// recorder that accepts everything would prove the tailer emitted a tombstone while proving nothing
// about whether the tombstone was allowed to win.
func startFleet(t *testing.T, cluster *harness.Cluster, stateDir string, idOffset uint32) *tailerFleet {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	fleet := &tailerFleet{cancel: cancel, done: make(chan struct{})}

	var wg sync.WaitGroup
	for _, sh := range cdcShards {
		tailer, err := cdc.New(cdc.Options{
			ShardID:         sh.id,
			Addr:            sh.addr,
			User:            "root",
			Password:        "cachet",
			Database:        "cachet",
			Table:           "entities",
			ServerID:        sh.serverID + idOffset,
			Cache:           cluster.Cache,
			Checkpoint:      cdc.NewFileCheckpoint(filepath.Join(stateDir, sh.id+".pos")),
			CheckpointEvery: 200 * time.Millisecond,
			OnInvalidate: func(_ string, _ uint64, applied bool) {
				if applied {
					fleet.applied.Add(1)
					return
				}
				fleet.rejected.Add(1)
			},
		})
		if err != nil {
			cancel()
			t.Fatalf("new tailer for %s: %v", sh.id, err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tailer.Run(ctx)
		}()
	}

	go func() {
		wg.Wait()
		close(fleet.done)
	}()

	t.Cleanup(fleet.stop)
	return fleet
}

// stop kills the fleet and waits for every tailer to exit, so a restarted fleet cannot race the
// generation it replaced for the same binlog connection.
func (f *tailerFleet) stop() {
	f.cancel()
	select {
	case <-f.done:
	case <-time.After(30 * time.Second):
	}
}

// writeRow writes straight to MySQL, bypassing Cachet entirely.
func writeRow(t *testing.T, db *sql.DB, id, version uint64, payload string) {
	t.Helper()

	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, 1, 0, ?, ?)
		 ON DUPLICATE KEY UPDATE payload = VALUES(payload), version = VALUES(version)`,
		id, payload, version); err != nil {
		t.Fatalf("write id=%d: %v", id, err)
	}
}

// eventually polls until cond holds, failing with why if it never does.
func eventually(t *testing.T, why string, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, why)
}

// readPayload reads a key through the engine at the default level.
func readPayload(t *testing.T, c cachetv1.CacheServiceClient, key string) (string, bool) {
	t.Helper()

	resp, err := c.Get(context.Background(), &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get %s: %v", key, err)
	}
	if !resp.GetFound() {
		return "", false
	}
	return string(resp.GetRecord().GetPayload()), true
}

// TestTailerRestartMidStreamLosesNoInvalidation is THE Phase 2 exit gate.
//
// The tailer is killed while writes are in flight and restarted from its checkpoint. Not one
// invalidation may be lost: every key written during the outage must stop serving its old value
// once the tailer catches up. A cache that silently kept those entries would be stale until their
// four-hour TTL, with nothing anywhere recording that it happened.
func TestTailerRestartMidStreamLosesNoInvalidation(t *testing.T) {
	ctx := context.Background()

	// Synchronous invalidation OFF: CDC is the only thing that can invalidate here, so a pass
	// cannot be the write path quietly doing CDC's job.
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: false,
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	dbs := make(map[string]*sql.DB, len(cdcShards))
	for _, sh := range cdcShards {
		dbs[sh.id] = openShardDB(t, sh.addr)
	}
	dbFor := func(id uint64) *sql.DB {
		shard, err := cluster.Router.ShardFor(fmt.Sprintf("entities:%d", id))
		if err != nil {
			t.Fatalf("route %d: %v", id, err)
		}
		return dbs[string(shard)]
	}

	const (
		base  = 9_700_000
		count = 60
	)
	keyOf := func(i int) string { return fmt.Sprintf("entities:%d", base+i) }

	// Seed every row and warm the cache, so each key has an entry that CDC will have to remove.
	version := uint64(time.Now().UnixMilli()) << 16
	next := func() uint64 { version++; return version }

	for i := 0; i < count; i++ {
		writeRow(t, dbFor(uint64(base+i)), uint64(base+i), next(), "original")
	}
	for i := 0; i < count; i++ {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: keyOf(i)}); err != nil {
			t.Fatalf("warm %s: %v", keyOf(i), err)
		}
	}
	for i := 0; i < count; i++ {
		if got, _ := readPayload(t, c, keyOf(i)); got != "original" {
			t.Fatalf("precondition: %s reads %q, want \"original\"", keyOf(i), got)
		}
	}

	stateDir := t.TempDir()
	first := startFleet(t, cluster, stateDir, 0)

	// Writes continue throughout, including across the kill. A restart that only works on a quiet
	// stream is a restart that has never met production.
	var (
		writeMu  sync.Mutex
		written  = map[int]string{}
		writerWG sync.WaitGroup
		stopping atomic.Bool
	)
	record := func(i int, payload string) {
		writeMu.Lock()
		written[i] = payload
		writeMu.Unlock()
	}

	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for i := 0; !stopping.Load() && i < count; i++ {
			payload := fmt.Sprintf("updated-%d", i)
			writeRow(t, dbFor(uint64(base+i)), uint64(base+i), next(), payload)
			record(i, payload)
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Let the first generation do real work, then kill it mid-stream.
	eventually(t, "the first tailer generation to invalidate something", 30*time.Second, func() bool {
		return first.applied.Load() > 0
	})
	first.stop()

	// Keep writing with NO tailer running. These are the events a naive restart would skip by
	// resuming from the current binlog position instead of the checkpoint.
	stopping.Store(true)
	writerWG.Wait()
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf("during-outage-%d", i)
		writeRow(t, dbFor(uint64(base+i)), uint64(base+i), next(), payload)
		record(i, payload)
	}

	// Before the second generation starts, the cache MUST still be serving the pre-outage values.
	// Without this the whole test could pass vacuously: if nothing were ever cached, every read
	// would fall through to the database, always return the current row, and the catch-up assertion
	// below would be satisfied by a cache that does nothing at all.
	stale := 0
	for i := 0; i < count; i++ {
		writeMu.Lock()
		want := written[i]
		writeMu.Unlock()
		if want == "" {
			continue
		}
		if got, found := readPayload(t, c, keyOf(i)); found && got != want {
			stale++
		}
	}
	if stale == 0 {
		t.Fatal("no key is stale before the tailer restarts; nothing was cached, so this test would " +
			"prove nothing about invalidation")
	}
	t.Logf("%d/%d keys are serving stale values before the restarted tailer catches up", stale, count)

	// A second generation resumes from the same checkpoints.
	second := startFleet(t, cluster, stateDir, 100)

	eventually(t, "every key written during the outage to stop serving a stale value", 90*time.Second, func() bool {
		for i := 0; i < count; i++ {
			writeMu.Lock()
			want := written[i]
			writeMu.Unlock()
			if want == "" {
				continue
			}
			got, found := readPayload(t, c, keyOf(i))
			if !found || got != want {
				return false
			}
		}
		return true
	})

	if second.applied.Load() == 0 {
		t.Error("the restarted tailer applied no invalidations; it did not resume from the checkpoint")
	}

	// Final assertion, read one more time so a pass cannot be an artefact of the polling loop
	// having caught a transient state.
	for i := 0; i < count; i++ {
		writeMu.Lock()
		want := written[i]
		writeMu.Unlock()
		if want == "" {
			continue
		}
		got, found := readPayload(t, c, keyOf(i))
		if !found {
			t.Errorf("%s is missing entirely", keyOf(i))
			continue
		}
		if got != want {
			t.Errorf("%s serves %q after the tailer caught up, want %q — an invalidation was lost",
				keyOf(i), got, want)
		}
	}
}

// TestReplayingTheBinlogCannotUndoNewerState is the idempotence half of the gate.
//
// A restarted tailer re-reads events it has already applied. That has to be a no-op, not a
// regression: the invariant is that a lower version never overwrites a higher one, so replaying an
// old event must LOSE its compare-and-set against a newer entry. Without this, every restart would
// briefly re-invalidate current data — and a tailer rewound far enough would wipe the cache.
func TestReplayingTheBinlogCannotUndoNewerState(t *testing.T) {
	ctx := context.Background()

	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: false,
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	dbs := make(map[string]*sql.DB, len(cdcShards))
	for _, sh := range cdcShards {
		dbs[sh.id] = openShardDB(t, sh.addr)
	}

	const id = 9_700_500
	key := fmt.Sprintf("entities:%d", id)
	shard, err := cluster.Router.ShardFor(key)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	db := dbs[string(shard)]

	version := uint64(time.Now().UnixMilli()) << 16
	next := func() uint64 { version++; return version }

	stateDir := t.TempDir()

	// Generation 1 starts FIRST. A tailer with no checkpoint begins at the current binlog position,
	// so a row written before it starts is a row it will never see — which is a property of
	// replication, not a bug, and a test that ignored it would be testing its own setup order.
	first := startFleet(t, cluster, stateDir, 200)
	cp := cdc.NewFileCheckpoint(filepath.Join(stateDir, string(shard)+".pos"))
	eventually(t, "the first tailer to record a starting checkpoint", 30*time.Second, func() bool {
		_, found, err := cp.Load()
		return err == nil && found
	})

	// Captured BEFORE either write, so generation 2 replays both of them.
	rewindTo, _, err := cp.Load()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}

	// An old write, invalidated and then cached.
	oldVersion := next()
	writeRow(t, db, id, oldVersion, "old")
	eventually(t, "the tailer to see the old write", 30*time.Second, func() bool {
		return first.applied.Load() > 0
	})
	if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("warm: %v", err)
	}
	eventually(t, "the old value to be cached", 15*time.Second, func() bool {
		got, found := readPayload(t, c, key)
		return found && got == "old"
	})

	// A newer write supersedes it.
	appliedBefore := first.applied.Load()
	writeRow(t, db, id, next(), "new")
	eventually(t, "the tailer to invalidate the old entry", 30*time.Second, func() bool {
		return first.applied.Load() > appliedBefore
	})
	eventually(t, "the new value to be cached", 15*time.Second, func() bool {
		got, found := readPayload(t, c, key)
		return found && got == "new"
	})
	first.stop()

	// Rewind this shard's checkpoint so generation 2 replays events it has already applied,
	// including the tombstone for the OLD write.
	if err := cp.Save(rewindTo); err != nil {
		t.Fatalf("rewind %s: %v", shard, err)
	}

	second := startFleet(t, cluster, stateDir, 300)
	eventually(t, "the replaying tailer to process the rewound events", 60*time.Second, func() bool {
		return second.applied.Load()+second.rejected.Load() > 0
	})

	// Give the replay time to do damage if it is going to.
	time.Sleep(3 * time.Second)

	// The newer value must still be there. If replay could undo newer state, this reads "old" or
	// misses — and every tailer restart would be a small, silent cache outage.
	got, ok := readPayload(t, c, key)
	if !ok {
		t.Fatal("the key is gone after a binlog replay; replay is not idempotent")
	}
	if got != "new" {
		t.Errorf("after replaying old events the key reads %q, want \"new\" — replay regressed the version", got)
	}

	if second.rejected.Load() == 0 {
		t.Error("the replaying tailer had no tombstone rejected; nothing proves the compare-and-set was exercised")
	}
}

// TestOutOfBandWritesAreInvalidatedByCDCAlone covers the case the write path structurally cannot:
// somebody changing a row without going through Cachet.
func TestOutOfBandWritesAreInvalidatedByCDCAlone(t *testing.T) {
	ctx := context.Background()

	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: false,
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	const id = 9_700_600
	key := fmt.Sprintf("entities:%d", id)
	shard, err := cluster.Router.ShardFor(key)
	if err != nil {
		t.Fatalf("route: %v", err)
	}

	var db *sql.DB
	for _, sh := range cdcShards {
		if sh.id == string(shard) {
			db = openShardDB(t, sh.addr)
		}
	}

	version := uint64(time.Now().UnixMilli()) << 16

	version++
	writeRow(t, db, id, version, "before")
	if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("warm: %v", err)
	}

	// The entry must actually be CACHED before the out-of-band write, or this test proves nothing:
	// a read that falls through to the database always returns the current row, and CDC would get
	// credit for work it never did.
	warm, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("second warm read: %v", err)
	}
	if !warm.GetMeta().GetCacheHit() {
		t.Fatal("the entry was not cached before the out-of-band write; this test would prove nothing")
	}
	if got := string(warm.GetRecord().GetPayload()); got != "before" {
		t.Fatalf("precondition: read %q, want \"before\"", got)
	}

	startFleet(t, cluster, t.TempDir(), 400)

	// A migration, an admin script, a backfill. Nothing told Cachet this happened.
	version++
	writeRow(t, db, id, version, "after")

	eventually(t, "CDC to notice a write that bypassed Cachet entirely", 60*time.Second, func() bool {
		got, found := readPayload(t, c, key)
		return found && got == "after"
	})
}
