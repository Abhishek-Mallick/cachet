//go:build e2e

package e2e_test

import (
	"context"
	"sync"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// The lease claim, stated so it can fail:
//
//	Origin load for a single key is bounded at roughly one fill per lease interval, regardless of
//	how many callers miss at once.
//
// This is the difference between deduplication and admission. A compare-and-set deduplicates: it
// decides whose fill WINS. Every one of those callers has already read the database by then. The
// lease decides who READS, which is the only thing that bounds what the origin sees.

// originReads reports how many reads have reached the database.
func originReads(t *testing.T, c *harness.Cluster) int {
	t.Helper()

	n, err := c.OriginReadsForTest()
	if err != nil {
		t.Fatalf("read origin counter: %v", err)
	}
	return n
}

// TestALeaseBoundsOriginLoadUnderAStampede is the headline result for leases.
func TestALeaseBoundsOriginLoadUnderAStampede(t *testing.T) {
	ctx := context.Background()

	// A lease interval comfortably longer than the burst, so the bound under test is "one fill",
	// not "one fill per interval, and the burst spanned three of them".
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
		LeaseTTL:                5 * time.Second,
		Leases:                  engine.NewWaitPolicy(8, 5*time.Millisecond, 100*time.Millisecond),
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_900_001)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("hot")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The write tombstoned the entry, so every one of the readers below starts from a genuine miss
	// on the same key — which is exactly the shape of a stampede: a hot key invalidated at peak.
	before := originReads(t, cluster)

	const readers = 500
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
				t.Errorf("Get: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	origin := originReads(t, cluster) - before

	waited := cluster.LeaseOutcomesForTest("waited")

	// This assertion is what makes the one below mean anything, and it replaces a control test that
	// turned out to be flaky.
	//
	// A low origin count has two possible causes: the lease held callers back, or the goroutines
	// never actually overlapped and a single early fill served everyone. Under `-race` the second
	// happens often — the scheduler serialises them — so a separate "without leases" control
	// measured a stampede that had not formed and failed for a reason unrelated to leases.
	//
	// Counting waits removes the ambiguity from the inside: a caller only waits because it found
	// another fill already in flight. If nobody waited, there was no stampede to prevent and this
	// test is not entitled to claim it prevented one.
	if waited == 0 {
		t.Fatal("no caller ever waited on another's fill, so the readers did not overlap and this " +
			"test did not observe a stampede — the origin count below would prove nothing")
	}

	// The bound is not exactly one: a caller whose wait is exhausted reads the origin itself, which
	// is the correct degradation and deliberately not suppressed. What must NOT happen is every
	// caller reaching the database.
	if origin >= readers {
		t.Fatalf("%d of %d concurrent readers reached the database; the lease bounded nothing",
			origin, readers)
	}
	if origin > readers/10 {
		t.Errorf("%d of %d concurrent readers reached the database, more than the 10%% ceiling this "+
			"test holds the claim to", origin, readers)
	}
	t.Logf("%d origin reads for %d concurrent readers of one key (%d waited on another's fill)",
		origin, readers, waited)
}

// TestWaitingIsWhatBoundsOriginLoad isolates the wait from the lease.
//
// With waiting disabled, a caller told another fill is in flight goes straight to the database —
// the pre-lease behaviour exactly. So any caller that would have waited becomes an origin read, and
// the two configurations differ by precisely the mechanism under test.
//
// It asserts the CONFIGURATION rather than a race outcome. An earlier version of this test counted
// origin reads under concurrency and was flaky under `-race`: the scheduler serialised the readers,
// no stampede formed, and it failed for a reason that had nothing to do with leases. A test that
// needs a race to happen is a test that will eventually fail when it does not.
func TestWaitingIsWhatBoundsOriginLoad(t *testing.T) {
	ctx := context.Background()

	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
		Leases:                  engine.NewWaitPolicy(0, 0, 0),
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_900_002)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("hot")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	const readers = 200
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: key}); err != nil {
				t.Errorf("Get: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	// Nobody may wait when waiting is switched off — whatever the scheduler did. Every caller that
	// met an in-flight fill went to the database instead, which is the cost the lease removes.
	if waited := cluster.LeaseOutcomesForTest("waited"); waited != 0 {
		t.Errorf("%d callers waited with wait_attempts=0; the setting does not disable waiting", waited)
	}
	t.Logf("%d origin reads for %d readers with waiting disabled", originReads(t, cluster), readers)
}

// TestAReaderIsNeverBlockedByADeadLeaseHolder is the safety property.
//
// A holder that dies is indistinguishable from one that is nearly finished. If waiting were
// unbounded, one crashed process would make the hottest key in the system unreadable — a
// self-inflicted outage strictly worse than the stampede the lease exists to prevent.
func TestAReaderIsNeverBlockedByADeadLeaseHolder(t *testing.T) {
	ctx := context.Background()

	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
		LeaseTTL:                30 * time.Second, // far longer than this test will wait
		Leases:                  engine.NewWaitPolicy(4, 10*time.Millisecond, 50*time.Millisecond),
	}, "tcp://127.0.0.1:0")
	c := cluster.Client(t, cluster.Addrs[0])

	key := "entities:" + itoa(9_900_003)
	if _, err := c.Put(ctx, &cachetv1.PutRequest{
		Key: key, Record: &cachetv1.Record{TenantId: 1, Payload: []byte("stuck")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Take the lease out of band and never release it: a filler that died mid-fill.
	res, err := cluster.Cache.GetOrLease(ctx, key)
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if res.Outcome.String() != "granted" {
		t.Fatalf("precondition: expected to take the lease, got %v", res.Outcome)
	}

	start := time.Now()
	resp, err := c.Get(ctx, &cachetv1.GetRequest{Key: key})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("a read behind a dead lease holder failed: %v", err)
	}
	if string(resp.GetRecord().GetPayload()) != "stuck" {
		t.Errorf("read %q, want \"stuck\"", resp.GetRecord().GetPayload())
	}
	// Four attempts backing off from 10ms to 50ms is well under a second even on a slow machine.
	if elapsed > 3*time.Second {
		t.Errorf("the read took %s behind a dead lease holder; waiting is not bounded and one "+
			"crashed process can stall a hot key", elapsed)
	}
}
