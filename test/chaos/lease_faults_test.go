//go:build chaos

package chaos_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// TestFault09LeaseHolderDiesDuringASlowFill — the failure mode leases introduce.
//
// A lease is the right to be the only caller reading the origin for a key. That bound is the whole
// point, and it is also a new way to fail: if the holder dies before filling, every other reader is
// waiting for a fill that will never happen. Handled badly, one dead process turns one hot key into
// a stalled key for the length of the lease TTL.
//
// The claim is that the wait is BOUNDED and degrades to a direct read. Callers get slower, they do
// not get stuck, and nothing returns an error.
func TestFault09LeaseHolderDiesDuringASlowFill(t *testing.T) {
	ctx := context.Background()

	// A lease TTL far longer than the test, so nothing here passes merely because the lease expired
	// on its own. The bounded wait has to be what rescues the readers.
	cluster := startProxied(ctx, t, harness.CacheOptions{
		SynchronousInvalidation: true,
		LeaseTTL:                60 * time.Second,
		Leases:                  engine.NewWaitPolicy(4, 10*time.Millisecond, 50*time.Millisecond),
	})
	c := cluster.Client(t, cluster.Addrs[0])

	const key = "entities:8800009"
	want := []byte("filled-by-nobody")
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: want},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The fault: take the lease and then die. Nothing will ever call FillWithLease with this token,
	// which is exactly what a filling process being killed looks like to every other reader.
	res, err := cluster.Cache.GetOrLease(ctx, key)
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if res.Outcome != cache.LeaseGranted {
		t.Fatalf("expected to be granted the lease so it could be abandoned, got outcome %v — "+
			"without holding it this test is not injecting anything", res.Outcome)
	}

	// "wait_exhausted" is the outcome that IS the claim: the bounded wait ran out and the reader
	// went to the origin instead of blocking on a fill that was never coming.
	exhaustedBefore := cluster.LeaseOutcomesForTest("wait_exhausted")
	waitedBefore := cluster.LeaseOutcomesForTest("waited")

	// Readers must not block on a fill that is never coming.
	const readers = 20
	start := time.Now()
	for i := 0; i < readers; i++ {
		got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
		if err != nil {
			t.Fatalf("read %d behind a dead lease holder failed: %v", i, err)
		}
		if string(got.GetRecord().GetPayload()) != string(want) {
			t.Fatalf("read %d returned %q, want %q", i, got.GetRecord().GetPayload(), want)
		}
	}
	elapsed := time.Since(start)

	// Non-vacuity: the readers must actually have encountered the abandoned lease. If none ever
	// waited, they were served some other way and nothing about the lease was exercised.
	exhausted := cluster.LeaseOutcomesForTest("wait_exhausted") - exhaustedBefore
	waited := cluster.LeaseOutcomesForTest("waited") - waitedBefore
	if waited == 0 {
		t.Fatal("no reader waited on the abandoned lease — the fault was not encountered, so this test proved nothing")
	}
	if exhausted == 0 {
		t.Fatal("no reader exhausted its wait and fell through to the origin: the readers were " +
			"rescued by something other than the bounded wait, which is the mechanism under test")
	}

	// The lease TTL is 60s. Anything close to that means callers were stalled on the holder rather
	// than degrading past it.
	if elapsed > 30*time.Second {
		t.Fatalf("%d reads took %v behind a dead lease holder: the wait is not bounded", readers, elapsed)
	}

	record(t, faultRecord{
		Number:    9,
		Title:     "Lease holder dies during a slow fill",
		Injection: "A caller takes the fill lease via GetOrLease and never fills, with a 60s lease TTL so expiry cannot rescue the readers",
		Claim:     "The wait is bounded and degrades to a direct read. Callers get slower; they do not get stuck, and none get an error.",
		Fired: fmt.Sprintf("`cachet_lease_outcomes_total{outcome=\"waited\"}` rose by %d and `{outcome=\"wait_exhausted\"}` by %d — the readers met the abandoned lease and their wait ran out",
			waited, exhausted),
		Observed: fmt.Sprintf("All %d reads returned the correct value in %v, against a lease TTL of 60s.", readers, elapsed.Round(time.Millisecond)),
		Explanation: "`cachet_lease_outcomes_total` separates granted, waited and wait_exhausted: rising `wait_exhausted` with no matching fill is the signature of a holder that died, " +
			"and `cachetctl inspect " + key + "` shows the key still unfilled while readers are served from the database.",
	})
}
