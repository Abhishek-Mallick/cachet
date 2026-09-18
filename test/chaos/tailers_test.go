//go:build chaos

package chaos_test

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/cdc"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// The tailers connect to the shards DIRECTLY, not through Toxiproxy. A fault aimed at the cache
// must not be confused by a tailer that also lost its database; when a fault targets a shard, the
// test says so explicitly.
//
// Server ids are offset clear of the other suites': two tailers sharing a replication id fight over
// the connection, and the resulting failure looks like a CDC bug rather than a collision.
var chaosShards = []struct {
	id       string
	addr     string
	serverID uint32
}{
	{"shard0", "127.0.0.1:3316", 4701},
	{"shard1", "127.0.0.1:3317", 4702},
	{"shard2", "127.0.0.1:3318", 4703},
}

// startTailers runs the CDC backstop for the duration of a test and does not return until every
// tailer is demonstrably streaming.
func startTailers(t *testing.T, cluster *harness.Cluster, retryFor time.Duration) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	stateDir := t.TempDir()

	seen := make(map[string]*atomic.Int64, len(chaosShards))
	for _, sh := range chaosShards {
		seen[sh.id] = &atomic.Int64{}
	}

	var wg sync.WaitGroup
	for _, sh := range chaosShards {
		counter := seen[sh.id]
		tailer, err := cdc.New(cdc.Options{
			ShardID:            sh.id,
			Addr:               sh.addr,
			User:               "root",
			Password:           "cachet",
			Database:           "cachet",
			Table:              "entities",
			ServerID:           sh.serverID,
			Cache:              cluster.Cache,
			Checkpoint:         cdc.NewFileCheckpoint(filepath.Join(stateDir, sh.id+".pos")),
			CheckpointEvery:    200 * time.Millisecond,
			InvalidateRetryFor: retryFor,
			OnInvalidate:       func(string, uint64, bool) { counter.Add(1) },
		})
		if err != nil {
			cancel()
			t.Fatalf("new tailer for %s: %v", sh.id, err)
		}

		wg.Add(1)
		go func(shard string) {
			defer wg.Done()
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

	waitForTailers(t, cluster, seen)
}

// waitForTailers writes canaries until every tailer has reacted.
//
// A tailer with no checkpoint starts at the CURRENT binlog position and cdc.New returns before the
// connection exists, so sleeping a fixed interval here is a race rather than a synchronisation: it
// passes on an idle machine and fails on a loaded one.
func waitForTailers(t *testing.T, cluster *harness.Cluster, seen map[string]*atomic.Int64) {
	t.Helper()

	ctx := context.Background()
	c := cluster.Client(t, cluster.Addrs[0])
	deadline := time.Now().Add(60 * time.Second)

	for round := 0; ; round++ {
		allSeen := true
		for _, counter := range seen {
			if counter.Load() == 0 {
				allSeen = false
			}
		}
		if allSeen {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tailers never attached: %v", seenCounts(seen))
		}
		// One canary per shard per round; the ring spreads these across all three.
		for i := 0; i < 12; i++ {
			key := "entities:" + itoa(8_900_000+round*100+i)
			if _, err := c.Put(ctx, &cachetv1.PutRequest{
				Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("canary")},
			}); err != nil {
				t.Fatalf("canary Put: %v", err)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func seenCounts(seen map[string]*atomic.Int64) map[string]int64 {
	out := make(map[string]int64, len(seen))
	for k, v := range seen {
		out[k] = v.Load()
	}
	return out
}
