//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// TestShutdownLosesNoAckedWrite is T8's second acceptance criterion.
//
// The failure it guards against is specific and severe: a write the database has already committed,
// whose ack the client never receives, during an ordinary rolling deploy. For a system whose
// headline claim is read-own-writes, that is the worst possible outcome — the client believes the
// write did not happen and will not know to re-read it.
//
// So the assertion is not "shutdown is quick". It is: every version the server ACKNOWLEDGED is
// durable in the database afterwards, and no acknowledged write is missing.
func TestShutdownLosesNoAckedWrite(t *testing.T) {
	ctx := context.Background()
	cluster := harness.Start(ctx, t, "tcp://127.0.0.1:0")
	client := cluster.Client(t, cluster.Addrs[0])

	const (
		writers              = 8
		writesPerHand        = 40
		idBase        uint64 = 9_300_000
	)

	type ack struct {
		id      uint64
		version uint64
	}

	var (
		mu   sync.Mutex
		acks []ack
		wg   sync.WaitGroup
	)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < writesPerHand; i++ {
				id := idBase + uint64(w*writesPerHand+i)
				resp, err := client.Put(ctx, &cachetv1.PutRequest{
					Key:    "entities:" + itoa(id),
					Record: &cachetv1.Record{TenantId: 1, Payload: []byte("shutdown")},
				})
				if err != nil {
					// A refused write during shutdown is correct behaviour: the client knows it
					// failed and can retry. Only an ACKNOWLEDGED write that vanished is a bug.
					continue
				}
				mu.Lock()
				acks = append(acks, ack{id: id, version: resp.GetMeta().GetVersion()})
				mu.Unlock()
			}
		}(w)
	}

	// Shut down while writes are still in flight, the way SIGTERM arrives mid-deploy.
	time.Sleep(25 * time.Millisecond)
	cluster.Stop()
	wg.Wait()

	if len(acks) == 0 {
		t.Fatal("no writes were acknowledged before shutdown; the test proved nothing")
	}
	t.Logf("%d writes acknowledged before and during shutdown", len(acks))

	for _, a := range acks {
		key := "entities:" + itoa(a.id)
		shardID, err := cluster.Router.ShardFor(key)
		if err != nil {
			t.Fatalf("ShardFor(%s): %v", key, err)
		}

		rec, _, err := cluster.Shards[shardID].Get(ctx, a.id)
		if errors.Is(err, storage.ErrNotFound) {
			t.Errorf("acknowledged write %s (version %d) is not in the database", key, a.version)
			continue
		}
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		if uint64(rec.Version) != a.version {
			t.Errorf("%s: stored version %d, acknowledged %d", key, rec.Version, a.version)
		}
	}
}

// TestShutdownStopsAcceptingNewWork asserts the other half of a clean drain: once shutdown begins,
// new requests are refused rather than accepted and then abandoned.
func TestShutdownStopsAcceptingNewWork(t *testing.T) {
	ctx := context.Background()
	cluster := harness.Start(ctx, t, "tcp://127.0.0.1:0")
	client := cluster.Client(t, cluster.Addrs[0])

	if _, err := client.Handshake(ctx, &cachetv1.HandshakeRequest{}); err != nil {
		t.Fatalf("Handshake before shutdown: %v", err)
	}

	cluster.Stop()

	deadline, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := client.Handshake(deadline, &cachetv1.HandshakeRequest{}); err == nil {
		t.Error("the engine served a request after shutdown completed")
	}
}
