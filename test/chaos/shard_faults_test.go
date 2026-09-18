//go:build chaos

package chaos_test

import (
	"context"
	"fmt"
	"testing"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// keysOnDistinctShards returns one key routed to target and one routed anywhere else.
func keysOnDistinctShards(t *testing.T, r *storage.Router, target storage.ShardID, base int) (on, off string) {
	t.Helper()

	for i := 0; i < 5000 && (on == "" || off == ""); i++ {
		key := "entities:" + itoa(base+i)
		id, err := r.ShardFor(key)
		if err != nil {
			t.Fatalf("ShardFor(%s): %v", key, err)
		}
		if id == target && on == "" {
			on = key
		}
		if id != target && off == "" {
			off = key
		}
	}
	if on == "" || off == "" {
		t.Fatalf("could not find keys on and off %s", target)
	}
	return on, off
}

// TestFault05ShardUnreachable — one database disappears.
//
// Two claims, and the second is the one worth injecting a fault to check. First: a read whose shard
// is gone fails, rather than inventing an answer — there is no honest cached reply to a request the
// origin cannot confirm. Second: the failure is CONTAINED. Keys on the other two shards keep being
// served, because a cache ring that followed the database's sharding would have turned one dead
// shard into a third of the keyspace going dark.
func TestFault05ShardUnreachable(t *testing.T) {
	ctx := context.Background()
	cluster := startProxied(ctx, t, harness.CacheOptions{SynchronousInvalidation: true})
	c := cluster.Client(t, cluster.Addrs[0])

	const target = storage.ShardID("shard1")
	dead, alive := keysOnDistinctShards(t, cluster.Router, target, 8_500_000)

	for _, k := range []string{dead, alive} {
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: k, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("pre-fault")},
		}); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	restore := disableProxy(ctx, t, string(target))
	defer restore()

	// A key whose shard is unreachable and whose entry is not cached has no honest answer.
	_, errDead := c.Get(ctx, &cachetv1.GetRequest{
		Key: dead, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG,
	})
	if errDead == nil {
		t.Fatalf("a STRONG read of %s succeeded while its shard was unreachable — STRONG means the "+
			"database confirmed it, so this answer was invented", dead)
	}

	// Containment: the rest of the keyspace is unaffected.
	got, err := c.Get(ctx, &cachetv1.GetRequest{
		Key: alive, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG,
	})
	if err != nil {
		t.Fatalf("a key on a healthy shard failed while %s was unreachable: %v — one dead shard "+
			"took out more than its own keys", target, err)
	}
	if string(got.GetRecord().GetPayload()) != "pre-fault" {
		t.Fatalf("healthy-shard read returned %q, want %q", got.GetRecord().GetPayload(), "pre-fault")
	}

	record(t, faultRecord{
		Number:      5,
		Title:       "Shard unreachable",
		Injection:   fmt.Sprintf("Toxiproxy: `POST /proxies/%s {\"enabled\": false}`", target),
		Claim:       "A read the origin cannot confirm fails rather than being invented, and the failure is contained to that shard's keys.",
		Fired:       fmt.Sprintf("The STRONG read of `%s` returned an error: %v", dead, errDead),
		Observed:    fmt.Sprintf("`%s` (on %s) failed; `%s` (on another shard) was served normally.", dead, target, alive),
		Explanation: fmt.Sprintf("`cachetctl locate %s` names the shard, and `cachetctl health` shows which shard is unreachable.", dead),
	})
}
