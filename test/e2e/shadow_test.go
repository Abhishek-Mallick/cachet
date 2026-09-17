//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/sextant"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// Shadow mode is the adoption wedge, and the milestone's stated exit criterion:
//
//	Sextant reports a consistency number for an application Cachet does not serve.
//
// Nobody adopts a cache on promises. Letting a team measure their OWN consistency, on their own
// traffic, before changing a line of application code is an offer no competitor in this category
// can make — because none of them can measure consistency at all. It is a first-class mode with its
// own tests rather than a script, because the demo it enables is the most compelling thing in the
// project and it must not rot.

// shadowVerifier builds a verifier over a cluster's real cache and shards, observing only.
func shadowVerifier(t *testing.T, cluster *harness.Cluster, keys *sextant.RecentKeys, now func() time.Time) (*sextant.Verifier, *sextant.SLO, *sextant.Tracer) {
	t.Helper()

	slo := sextant.NewSLO(time.Hour)
	tracer := sextant.NewTracer(sextant.TracerOptions{})

	v, err := sextant.NewVerifier(sextant.VerifierOptions{
		Cache:  sextant.NewCacheAdapter(cluster.Cache),
		Origin: sextant.NewOriginAdapter(cluster.Router, cluster.Shards),
		Keys:   keys,
		// The bound the harness engine actually promises. Summed from settings rather than picked,
		// so the verifier and the engine cannot disagree about what was promised.
		Bound:  sextant.NewPropagationBound(50*time.Millisecond, 5*time.Second, 250*time.Millisecond),
		SLO:    slo,
		Tracer: tracer,
		Batch:  200,
		Shadow: true,
		Now:    now,
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return v, slo, tracer
}

// TestShadowModeReportsANumberForAHealthySystem is the milestone's exit criterion.
func TestShadowModeReportsANumberForAHealthySystem(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	keys := sextant.NewRecentKeys(1000)
	now := time.Now()
	v, slo, _ := shadowVerifier(t, cluster, keys, func() time.Time { return now })

	// Ordinary application traffic: write, then read so the entry is cached.
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("entities:%d", 9_950_000+i)
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("Get: %v", err)
		}
		keys.Touch(key)
	}

	round := v.RunOnce(ctx)
	if round.Checked == 0 {
		t.Fatal("the verifier checked nothing; it cannot report a number about a system it did not look at")
	}
	if round.Violations != 0 {
		t.Errorf("%d violations on a healthy system with synchronous invalidation on", round.Violations)
	}

	// The deliverable, stated as the milestone states it: a number, per level, with the evidence
	// count beside it.
	for _, level := range []consistency.Level{consistency.Session, consistency.Bounded, consistency.Eventual} {
		r := slo.Report(level, now)
		if !r.Known {
			t.Errorf("%s reports no known figure after %d checks", level, round.Checked)
			continue
		}
		if r.Consistency != 1 {
			t.Errorf("%s = %v on a healthy system, want 1", level, r.Consistency)
		}
		t.Logf("%-8s consistency=%.6f nines=%.2f observations=%d violations=%d",
			level, r.Consistency, r.Nines, r.Observations, r.Violations)
	}
}

// TestShadowModeDetectsRealStaleness is the other half, and the one that makes the number mean
// something.
//
// A verifier that reports 100% on a healthy system and 100% on a broken one is a verifier that
// reports 100%. This drives the cache into a genuinely stale state and requires Sextant to say so.
func TestShadowModeDetectsRealStaleness(t *testing.T) {
	ctx := context.Background()

	// Invalidation OFF: writes never reach the cache, so entries go stale and stay stale. This is
	// the Phase 1 naive cache reproduced deliberately — a system whose staleness is real, bounded
	// only by the TTL, and therefore exactly what a verifier must be able to catch.
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: false,
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	keys := sextant.NewRecentKeys(1000)
	now := time.Now()
	clock := func() time.Time { return now }
	v, slo, _ := shadowVerifier(t, cluster, keys, clock)

	const n = 40
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("entities:%d", 9_960_000+i)
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		// Cache it, then write again. With invalidation off the cached entry keeps the old version.
		if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v2")},
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}
		keys.Touch(key)
	}

	// First round establishes when each key was seen behind. Within the propagation bound this is
	// an in-flight race, not a violation — and the verifier must not report one yet.
	first := v.RunOnce(ctx)
	if first.Violations != 0 {
		t.Errorf("%d violations reported immediately; entries behind for zero time are in-flight "+
			"races, which the model explicitly permits", first.Violations)
	}

	// Past the bound, "in flight" stops being an explanation.
	now = now.Add(6 * time.Second)
	second := v.RunOnce(ctx)

	if second.Violations == 0 {
		t.Fatal("no violations reported against a cache with invalidation disabled; the verifier " +
			"cannot detect staleness and every number it publishes is worthless")
	}
	t.Logf("%d violations detected across %d checks", second.Violations, second.Checked)

	r := slo.Report(consistency.Eventual, now)
	if r.Consistency == 1 {
		t.Error("EVENTUAL still reports perfect consistency despite detected violations")
	}
	t.Logf("EVENTUAL consistency=%.4f nines=%.2f observations=%d violations=%d",
		r.Consistency, r.Nines, r.Observations, r.Violations)
}

// TestShadowModeNeverMutatesTheCache is the safety property that makes shadow mode adoptable.
//
// The entire offer is "run this against your system and nothing changes". A verifier that repaired
// the staleness it found would both break that promise and destroy its own evidence — the number it
// published would be proof that it had interfered.
func TestShadowModeNeverMutatesTheCache(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: false,
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:9970001"
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v2")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	before, hit, err := cluster.Cache.Get(ctx, key)
	if err != nil || !hit {
		t.Fatalf("precondition: the entry should be cached (hit=%v err=%v)", hit, err)
	}

	keys := sextant.NewRecentKeys(10)
	keys.Touch(key)
	now := time.Now().Add(time.Hour) // well past the propagation bound
	v, _, _ := shadowVerifier(t, cluster, keys, func() time.Time { return now })
	v.RunOnce(ctx)
	v.RunOnce(ctx)

	after, hit, err := cluster.Cache.Get(ctx, key)
	if err != nil || !hit {
		t.Fatalf("the entry disappeared while being verified (hit=%v err=%v); shadow mode changed "+
			"the system it was only supposed to observe", hit, err)
	}
	if after.FillVersion != before.FillVersion || after.RowVersion != before.RowVersion {
		t.Errorf("the entry changed while being verified: before %+v, after %+v", before, after)
	}
}

// TestAViolationCarriesATraceThatExplainsIt is the difference between a monitor and a verifier.
func TestAViolationCarriesATraceThatExplainsIt(t *testing.T) {
	ctx := context.Background()
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: false,
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:9980001"
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v1")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("v2")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	keys := sextant.NewRecentKeys(10)
	keys.Touch(key)
	now := time.Now()
	slo := sextant.NewSLO(time.Hour)
	tracer := sextant.NewTracer(sextant.TracerOptions{})

	// The mutations Sextant would have observed from the binlog.
	tracer.Record(key, sextant.Event{
		Op: sextant.OpFill, Version: 1, At: now, Source: sextant.SourceReadFill, Actor: "engine-1",
	})
	tracer.Record(key, sextant.Event{
		Op: sextant.OpTombstone, Version: 2, At: now, Source: sextant.SourceCDC, Actor: "shard1",
	})

	var got sextant.Violation
	v, err := sextant.NewVerifier(sextant.VerifierOptions{
		Cache:  sextant.NewCacheAdapter(cluster.Cache),
		Origin: sextant.NewOriginAdapter(cluster.Router, cluster.Shards),
		Keys:   keys,
		Bound:  sextant.NewPropagationBound(50*time.Millisecond, 5*time.Second, 250*time.Millisecond),
		SLO:    slo, Tracer: tracer, Batch: 10, Shadow: true,
		Now:         func() time.Time { return now },
		OnViolation: func(vi sextant.Violation) { got = vi },
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	v.RunOnce(ctx)
	now = now.Add(time.Minute)
	v.RunOnce(ctx)

	if got.Key == "" {
		t.Fatal("no violation was reported for a key that is definitely stale")
	}
	if len(got.Trace) == 0 {
		t.Fatal("the violation carries no trace; it says a key was stale and nothing about why, " +
			"which is a monitor rather than a verifier")
	}
	t.Logf("violation: %s", got)
	for _, e := range got.Trace {
		t.Logf("  trace: %s", e)
	}
}
