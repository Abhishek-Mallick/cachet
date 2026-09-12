//go:build e2e

package conformance_test

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/cdc"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// Most conformance cells run without CDC, because the guarantee they test is carried by the write
// path and the session watermark. The degraded cells are the exception: a degraded write deliberately
// leaves its keys to the tailer, so without one running there is nothing to converge and
// "BOUNDED(cdc_lag_bound)" would be an unfalsifiable claim.

// conformanceShards matches test/env/compose.yml. Server ids are offset well clear of the e2e
// suite's, because two tailers sharing a replication id fight over the connection.
var conformanceShards = []struct {
	id       string
	addr     string
	serverID uint32
}{
	{"shard0", "127.0.0.1:3316", 4601},
	{"shard1", "127.0.0.1:3317", 4602},
	{"shard2", "127.0.0.1:3318", 4603},
}

// startTailers runs one tailer per shard against the environment's real cache, and does not return
// until every one of them is demonstrably streaming.
func startTailers(t *testing.T, e *env) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	stateDir := t.TempDir()

	// seen counts invalidations per shard, which is the only honest signal that a tailer is
	// attached: cdc.New returns before the binlog connection is established, and Run does the
	// connecting asynchronously.
	seen := make(map[string]*atomic.Int64, len(conformanceShards))
	for _, sh := range conformanceShards {
		seen[sh.id] = &atomic.Int64{}
	}

	var wg sync.WaitGroup
	for _, sh := range conformanceShards {
		counter := seen[sh.id]
		tailer, err := cdc.New(cdc.Options{
			ShardID:         sh.id,
			Addr:            sh.addr,
			User:            "root",
			Password:        "cachet",
			Database:        "cachet",
			Table:           "entities",
			ServerID:        sh.serverID,
			Cache:           e.cluster.Cache,
			Checkpoint:      cdc.NewFileCheckpoint(filepath.Join(stateDir, sh.id+".pos")),
			CheckpointEvery: 200 * time.Millisecond,
			OnInvalidate:    func(string, uint64, bool) { counter.Add(1) },
		})
		if err != nil {
			cancel()
			t.Fatalf("new tailer for %s: %v", sh.id, err)
		}

		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
			// The error is reported, not discarded. A tailer that fails to start would otherwise
			// present as "CDC did not converge", sending anyone debugging it to look at
			// invalidation instead of at the connection that never opened.
			if err := tailer.Run(ctx); err != nil && ctx.Err() == nil {
				t.Errorf("tailer for %s stopped: %v", shard, err)
			}
		}(sh.id)
	}

	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("tailers did not shut down")
		}
	})

	waitForTailers(t, e, seen)
}

// waitForTailers blocks until every shard's tailer has processed a row event.
//
// A tailer with no checkpoint starts at the CURRENT binlog position, so it cannot see a write that
// happened before it connected — and cdc.New returns before that connection exists. Sleeping a
// fixed interval here is a race rather than a synchronisation: it passes on a fast machine and
// fails on a loaded one, which is exactly the flake this replaced. Writing canaries until each
// tailer reacts proves the thing that actually matters.
func waitForTailers(t *testing.T, e *env, seen map[string]*atomic.Int64) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	canary := uint64(7_900_000_000)

	for {
		pending := make([]string, 0, len(seen))
		for shard, counter := range seen {
			if counter.Load() == 0 {
				pending = append(pending, shard)
			}
		}
		if len(pending) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tailers for %v never processed a row event; CDC is not streaming and any "+
				"convergence assertion below would be measuring the wrong thing", pending)
		}

		// Written straight to the shard, bypassing the engine, so the canary provokes a binlog event
		// without the write path invalidating anything and muddying the signal.
		for _, shard := range pending {
			canary++
			sh, ok := e.cluster.Shards[storage.ShardID(shard)]
			if !ok {
				t.Fatalf("no open shard %s", shard)
			}
			if _, err := sh.Put(context.Background(), storage.Record{
				ID: canary, TenantID: 65000, Status: 1, Payload: []byte("canary"),
			}); err != nil {
				t.Fatalf("canary write to %s: %v", shard, err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
