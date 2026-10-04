//go:build chaos

package chaos_test

import (
	"context"
	"testing"
	"time"

	cachetv2 "github.com/Abhishek-Mallick/cachet/api/cachet/v2"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/test/fixtures/table"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// Fault 10: the cache node a key lives on stops answering.
//
// Cachet had two answers for this and both protect the same thing. The per-node circuit breaker
// stops a dying node holding requests open; the read path treats a cache error as a miss and reads
// the database. Both protect LATENCY.
//
// Neither protects the ORIGIN. The moment a node goes quiet its entire share of the keyspace
// arrives at the database — with three cache nodes, a third of all reads, at once, against a
// database with no breaker of its own. The gutter pool is what stands between those two facts.
//
// The fault is injected by cutting Toxiproxy's route to the cache. The primary reaches it through
// the proxy and the gutter reaches it directly, so one address becomes genuinely unreachable while
// the other stays up — which is the shape of a real node failure rather than a simulation of one.

const (
	// The gutter talks to the cache directly, bypassing the proxy that is about to be cut.
	gutterAddr = "127.0.0.1:6379"

	// Long enough that nothing expires mid-test, short enough to be a bound somebody would accept.
	gutterTTL = 30 * time.Second
)

func gutterCluster(ctx context.Context, t *testing.T) *harness.Cluster {
	t.Helper()

	return startProxied(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
		GutterAddresses:         []string{gutterAddr},
		GutterTTL:               gutterTTL,
	})
}

func putRow(ctx context.Context, t *testing.T, v2 cachetv2.CacheServiceClient, id uint64, payload string) *cachetv2.PutResponse {
	t.Helper()

	resp, err := v2.Put(ctx, &cachetv2.PutRequest{
		Key: table.Key(id).String(), Row: rowToProto(table.Row(id, 1, 0, payload)),
	})
	if err != nil {
		t.Fatalf("Put %d: %v", id, err)
	}
	return resp
}

func rowToProto(row []schema.Value) *cachetv2.Row {
	out := &cachetv2.Row{Values: make([]*cachetv2.Value, 0, len(row))}
	for _, v := range row {
		if v.IsNull {
			out.Values = append(out.Values, &cachetv2.Value{IsNull: true})
			continue
		}
		out.Values = append(out.Values, &cachetv2.Value{Data: v.Bytes})
	}
	return out
}

// TestFault10GutterAbsorbsADeadCacheNode is the claim, measured on the origin counter.
//
// "The cache still answered" is not the property. The property is that the database did not receive
// the dead node's whole share of the keyspace, so the assertion is on origin reads: twenty reads of
// ten keys reach it ten times with a gutter and twenty without.
func TestFault10GutterAbsorbsADeadCacheNode(t *testing.T) {
	ctx := context.Background()
	cluster := gutterCluster(ctx, t)
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	const (
		keys  = 10
		first = 9_900_100
	)
	for i := range keys {
		putRow(ctx, t, v2, uint64(first+i), "v1")
	}

	restore := disableProxy(ctx, t, "cache")
	defer restore()

	// Evidence the injection fired: with the route cut, the engine's own counter of failed cache
	// reads has to move. A fault that silently did not apply produces a green test asserting
	// nothing.
	errorsBefore := cluster.CacheOpsForTest("get", "error")
	originBefore, err := cluster.OriginReadsForTest()
	if err != nil {
		t.Fatalf("origin reads: %v", err)
	}

	// Each key twice. The first read finds the home node gone and the gutter empty, so it reaches
	// the database and fills the standby; the second must not.
	for pass := range 2 {
		for i := range keys {
			got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: table.Key(uint64(first + i)).String()})
			if err != nil {
				t.Fatalf("pass %d Get %d: %v", pass, first+i, err)
			}
			if !got.GetFound() {
				t.Fatalf("pass %d: %d was not found while the cache node was down", pass, first+i)
			}
		}
	}

	cacheErrors := cluster.CacheOpsForTest("get", "error") - errorsBefore
	if cacheErrors == 0 {
		t.Fatal("no cache read failed while the route was cut; the fault did not fire and every " +
			"assertion below would be about a healthy system")
	}

	originAfter, err := cluster.OriginReadsForTest()
	if err != nil {
		t.Fatalf("origin reads: %v", err)
	}
	reads := originAfter - originBefore

	// Generous by two: a lease loss or a retry can add one, and the number that matters is how far
	// this sits below the twenty a gutterless engine would produce.
	if reads > keys+2 {
		t.Errorf("%d origin reads for %d keys read twice with a gutter configured; without one it "+
			"would be %d, so the gutter is not absorbing them", reads, keys, 2*keys)
	}

	record(t, faultRecord{
		Number:    10,
		Title:     "A cache node stops answering",
		Injection: "Toxiproxy route to the cache disabled; the gutter pool reaches it directly",
		Claim: "A dead node's keys are absorbed by the gutter pool instead of arriving at the " +
			"database all at once. The breaker protects latency; this protects the origin.",
		Fired: "cachet_cache_ops_total{op=\"get\",result=\"error\"} rose by " +
			itoa(cacheErrors) + " while the route was cut",
		Observed: itoa(reads) + " origin reads for " + itoa(keys) +
			" keys read twice each; without a gutter it would be " + itoa(2*keys),
		Explanation: "cachet_cache_ops_total{op=\"gutter\"} separates hits, misses and fills in the " +
			"standby pool from the primary's, so an operator can see the gutter working rather " +
			"than infer it from an absence of load",
	})
}

// TestFault10AGutterReadDeclaresItself. A weakened answer nobody is told about is worse than a slow
// one: nothing invalidates a gutter entry, so its staleness is bounded by the gutter TTL and the
// caller needs that number to decide whether to accept it.
func TestFault10AGutterReadDeclaresItself(t *testing.T) {
	ctx := context.Background()
	cluster := gutterCluster(ctx, t)
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	const id = 9_900_200
	putRow(ctx, t, v2, id, "v1")

	restore := disableProxy(ctx, t, "cache")
	defer restore()

	key := table.Key(id).String()
	// The first read fills the gutter from the origin; the second is served by it.
	if _, err := v2.Get(ctx, &cachetv2.GetRequest{Key: key}); err != nil {
		t.Fatalf("filling Get: %v", err)
	}
	got, err := v2.Get(ctx, &cachetv2.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !got.GetMeta().GetCacheHit() {
		t.Fatal("the second read was not served from a cache at all; there is nothing to assert about")
	}
	if !got.GetMeta().GetDegraded() {
		t.Error("a read served from the gutter did not report degraded; the caller has no way to " +
			"know it was answered from a pool nothing invalidates")
	}
	if got.GetMeta().GetDegradedReason() == "" {
		t.Error("the degraded read carries no reason")
	}
	if bound := got.GetMeta().GetEffectiveStalenessBound().AsDuration(); bound != gutterTTL {
		t.Errorf("effective staleness bound = %s, want the gutter TTL %s; a degraded read without "+
			"a number is one the caller cannot act on", bound, gutterTTL)
	}
}

// TestFault10ReadOwnWritesSurvivesTheGutter is the guarantee that must not bend.
//
// A gutter entry is never invalidated — the write path tombstones the home node, and the home node
// is the one that is down — so the only thing between a session and its own superseded write is the
// watermark. If this cell fails, the gutter is a way around the consistency model rather than a
// pool inside it.
func TestFault10ReadOwnWritesSurvivesTheGutter(t *testing.T) {
	ctx := context.Background()
	cluster := gutterCluster(ctx, t)
	v2 := cluster.ClientV2(t, cluster.Addrs[0])

	const id = 9_900_300
	key := table.Key(id).String()
	putRow(ctx, t, v2, id, "v1")

	restore := disableProxy(ctx, t, "cache")
	defer restore()

	// Fill the gutter with v1 while the home node is unreachable.
	if _, err := v2.Get(ctx, &cachetv2.GetRequest{Key: key}); err != nil {
		t.Fatalf("filling Get: %v", err)
	}

	// Write v2. Its invalidation cannot reach the home node, which is the point: the gutter is left
	// holding v1 and only the session token can stop it being served.
	w := putRow(ctx, t, v2, id, "v2")

	got, err := v2.Get(ctx, &cachetv2.GetRequest{
		Key:     key,
		Level:   cachetv2.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
		Session: w.GetSession(),
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("the row was not found")
	}

	payload := got.GetRow().GetValues()[3].GetData()
	if string(payload) != "v2" {
		t.Errorf("a session read its own write as %q; the gutter served an entry older than the "+
			"session's watermark, which is the one thing it must never do", payload)
	}
}
