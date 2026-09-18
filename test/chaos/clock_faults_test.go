//go:build chaos

package chaos_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// TestFault08ClockSkewBetweenShards — one shard's clock runs ahead of the others.
//
// Versions are hybrid logical clock timestamps, and clock skew is the failure HLCs exist to absorb.
// The design's answer (ADR 0003) is that versions are comparable WITHIN a shard and meaningless
// across shards, and that a session's position is therefore a sparse map from shard to version
// rather than a scalar. This fault is what makes that claim falsifiable: with one shard three
// seconds ahead, a scalar watermark would compare incomparable numbers and either lose
// read-own-writes on the slow shards or pin them to a future version and serve nothing.
//
// The skew injected here is an order of magnitude beyond the engine's configured tolerance, which
// is the point: read-own-writes is carried by the watermark, not by the clocks agreeing.
func TestFault08ClockSkewBetweenShards(t *testing.T) {
	ctx := context.Background()

	const skew = 3 * time.Second
	cluster := startProxied(ctx, t, harness.CacheOptions{
		SynchronousInvalidation: true,
		ClockSkew: func(shard string) time.Duration {
			if shard == "shard0" {
				return skew
			}
			return 0
		},
	})
	c := cluster.Client(t, cluster.Addrs[0])

	skewed := keysOn(t, cluster.Router, storage.ShardID("shard0"), 8_300_000, 1)[0]
	var normal string
	for i := 0; i < 5000 && normal == ""; i++ {
		k := "entities:" + itoa(8_310_000+i)
		id, err := cluster.Router.ShardFor(k)
		if err != nil {
			t.Fatalf("ShardFor: %v", err)
		}
		if id != storage.ShardID("shard0") {
			normal = k
		}
	}
	if normal == "" {
		t.Fatal("no key routed off shard0")
	}

	writeAt := time.Now()
	putSkewed, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: skewed, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("on-the-fast-clock")},
	})
	if err != nil {
		t.Fatalf("Put on the skewed shard: %v", err)
	}
	putNormal, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: normal, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("on-a-normal-clock")},
	})
	if err != nil {
		t.Fatalf("Put on a normal shard: %v", err)
	}

	// Non-vacuity: the skew has to have reached the versions, or nothing was injected. The physical
	// component of an HLC version is where a shard's clock actually shows up.
	skewedAt := storage.Version(putSkewed.GetMeta().GetVersion()).Time()
	normalAt := storage.Version(putNormal.GetMeta().GetVersion()).Time()
	observed := skewedAt.Sub(normalAt)
	if observed < skew/2 {
		t.Fatalf("the skewed shard's version is only %v ahead of the normal shard's (want ~%v): "+
			"the clock skew never reached the versions, so this test injected nothing", observed, skew)
	}
	if skewedAt.Before(writeAt) {
		t.Fatalf("version time %v precedes the write at %v", skewedAt, writeAt)
	}

	// Claim 1: read-own-writes holds on the shard whose clock is wrong.
	sess := putSkewed.GetSession()
	got, err := c.Get(ctx, &cachetv1.GetRequest{
		Key: skewed, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION, Session: sess,
	})
	if err != nil {
		t.Fatalf("SESSION read on the skewed shard: %v", err)
	}
	if p := string(got.GetRecord().GetPayload()); p != "on-the-fast-clock" {
		t.Fatalf("read-own-writes broke on the skewed shard: got %q", p)
	}

	// Claim 2: the skew does not leak. A session that has touched the fast shard must not drag the
	// slow shards' watermarks forward — that is precisely what a scalar version would do, and it
	// would make every read on those shards wait for a version that will not exist for 3 seconds.
	putNormal2, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: normal, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("second-write")},
		Session: putSkewed.GetSession(),
	})
	if err != nil {
		t.Fatalf("Put on the normal shard carrying the skewed session: %v", err)
	}
	got, err = c.Get(ctx, &cachetv1.GetRequest{
		Key: normal, Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
		Session: putNormal2.GetSession(),
	})
	if err != nil {
		t.Fatalf("SESSION read on the normal shard after touching the skewed one: %v", err)
	}
	if p := string(got.GetRecord().GetPayload()); p != "second-write" {
		t.Fatalf("a session that touched the fast shard broke reads on a normal shard: got %q", p)
	}

	// Claim 3: versions stay monotonic on the skewed shard across successive writes.
	last := storage.Version(putSkewed.GetMeta().GetVersion())
	for i := 0; i < 5; i++ {
		res, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: skewed, Record: &cachetv1.Record{TenantId: 1, Payload: []byte(fmt.Sprintf("w%d", i))},
		})
		if err != nil {
			t.Fatalf("successive Put %d: %v", i, err)
		}
		if v := storage.Version(res.GetMeta().GetVersion()); v <= last {
			t.Fatalf("version went backwards on the skewed shard: %d then %d", last, v)
		} else {
			last = v
		}
	}

	record(t, faultRecord{
		Number:    8,
		Title:     "Clock skew between shards",
		Injection: fmt.Sprintf("shard0's clock is advanced %v — an order of magnitude beyond the engine's configured tolerance — while the other shards run normally", skew),
		Claim:     "Read-own-writes is carried by the per-shard session watermark, not by clocks agreeing. Versions stay monotonic within the skewed shard and the skew does not leak to the others.",
		Fired:     fmt.Sprintf("Versions written to shard0 carry a physical time %v ahead of those written to an unskewed shard", observed.Round(time.Millisecond)),
		Observed:  "SESSION reads returned the caller's own writes on both the skewed and the normal shard, and six successive writes to the skewed shard produced strictly increasing versions.",
		Explanation: "`cachetctl inspect " + skewed + "` prints the row version and the entry's fill version; the session token is a per-shard map, " +
			"so a version from the fast shard is never compared against one from a slow shard. " +
			"Note what is NOT claimed: BOUNDED(t) sums `max_clock_skew` into its propagation bound, so skew beyond the configured value is outside the guarantee by construction, not absorbed by it.",
	})
}
