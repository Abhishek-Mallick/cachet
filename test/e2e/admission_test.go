//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/admission"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// Adaptive admission through the real engine. The unit tests prove the policy does not oscillate;
// these prove the engine actually consults it, and that the two directions both work — because a
// mechanism that only ever evicts is a ratchet, and one that only ever admits is not doing anything.

func admittingCluster(t *testing.T, opts admission.PolicyOptions) *harness.Cluster {
	t.Helper()

	ctrl := admission.NewController(admission.ControllerOptions{
		Sketch: admission.NewSketch(admission.SketchOptions{Window: time.Minute}),
		Policy: admission.NewPolicy(opts),
	})
	return harness.StartCachedWith(context.Background(), t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
		Admission:               ctrl,
	}, "tcp://127.0.0.1:0")
}

// TestAWriteChurningKeyStopsBeingCached is the milestone's claim, through the engine.
func TestAWriteChurningKeyStopsBeingCached(t *testing.T) {
	ctx := context.Background()
	cluster := admittingCluster(t, admission.PolicyOptions{
		AdmitRatio: 20, EvictRatio: 10, MinSamples: 20,
		MinDwell: 0, DefaultAdmit: true,
	})
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_990_001)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// A key taking as many writes as reads. Every write pays invalidation and every read misses —
	// caching it is pure cost, and a human choosing tables would never find it.
	for i := 0; i < 40; i++ {
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v")},
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}

	// Once evicted from admission, reads stop being served from cache — they read through instead.
	// The value is still correct; what changed is who paid for it.
	resp, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetMeta().GetCacheHit() {
		t.Error("a 1:1 read:write key is still being served from cache; admission is not consulted " +
			"on the read path")
	}
	// Correctness is unaffected — admission decides who pays, never what a read returns.
	if string(resp.GetRecord().GetPayload()) != "v" {
		t.Errorf("read %q, want \"v\"; admission changed the ANSWER, which it must never do",
			resp.GetRecord().GetPayload())
	}
}

// TestAReadHeavyKeyKeepsBeingCached is the other direction, and the control.
//
// Without it, the test above passes for a broken controller that refuses everything.
func TestAReadHeavyKeyKeepsBeingCached(t *testing.T) {
	ctx := context.Background()
	cluster := admittingCluster(t, admission.PolicyOptions{
		AdmitRatio: 20, EvictRatio: 10, MinSamples: 20,
		MinDwell: 0, DefaultAdmit: true,
	})
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_990_002)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	for i := 0; i < 100; i++ {
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}

	resp, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetMeta().GetCacheHit() {
		t.Error("a read-only key is not being served from cache; admission is refusing everything")
	}
}

// TestAdmissionNeverChangesWhatAReadReturns is the safety property.
//
// Admission is a COST decision. An uncached read is served from the database, which is at least as
// fresh as the cache would have been, so no guarantee is affected. If that were ever untrue,
// admission would be quietly weakening the consistency model from a performance setting.
func TestAdmissionNeverChangesWhatAReadReturns(t *testing.T) {
	ctx := context.Background()
	cluster := admittingCluster(t, admission.PolicyOptions{
		// Impossible to satisfy: nothing is ever admitted. The strongest version of the property.
		AdmitRatio: 1e9, EvictRatio: 1e8, MinSamples: 1,
		MinDwell: 0, DefaultAdmit: false,
	})
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_990_003)
	for i, want := range []string{"v1", "v2", "v3"} {
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: key, Record: &cachetv1.Record{TenantId: 1, Status: uint32(i + 1), Payload: []byte(want)},
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		resp, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got := string(resp.GetRecord().GetPayload()); got != want {
			t.Errorf("read %q, want %q; admission changed the answer rather than only the cost", got, want)
		}
		if resp.GetMeta().GetCacheHit() {
			t.Error("a cache hit with admission refusing everything")
		}
	}
}

// TestAdmissionOffCachesEverything pins the default.
//
// Off by default, and deliberately: every benchmark row recorded before this existed was measured
// with everything cached. Turning it on silently would make new rows incomparable with old ones
// without anyone noticing that the configuration under test had changed.
func TestAdmissionOffCachesEverything(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_990_004)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// A 1:1 key, which adaptive admission would evict. With admission off it stays cached.
	for i := 0; i < 40; i++ {
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v")},
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	resp, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetMeta().GetCacheHit() {
		t.Error("a key was not cached with admission disabled; the default changed")
	}
}
