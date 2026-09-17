//go:build integration

package cache_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
)

// A lease is ADMISSION control, not a correctness mechanism. Deduplicating concurrent fills — which
// the compare-and-set already does — fixes ORDERING: a slow fill cannot clobber a newer value. It
// does nothing about admission, so ten thousand simultaneous misses on one hot key all still reach
// the database, at exactly the moment you can least afford them.
//
// The lease fixes that, and the claim it has to support is specific:
//
//	Origin load for a single key is bounded at roughly one fill per lease interval, regardless of
//	how many callers miss at once.
//
// Everything in this file exists to hold that claim to account, including the two ways it could go
// wrong in production: a holder that dies mid-fill, and a caller that waits forever.

func TestALeaseIsGrantedOnAMiss(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	res, err := c.GetOrLease(ctx, "lease:granted")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if res.Outcome != cache.LeaseGranted {
		t.Fatalf("Outcome = %v on an empty key, want LeaseGranted", res.Outcome)
	}
	if res.Token == "" {
		t.Error("a granted lease carries no token; the holder could not prove it owns the fill")
	}
}

func TestOnlyOneCallerIsGrantedALease(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	first, err := c.GetOrLease(ctx, "lease:exclusive")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if first.Outcome != cache.LeaseGranted {
		t.Fatalf("the first caller got %v, want LeaseGranted", first.Outcome)
	}

	// The second caller must be told to WAIT, not handed a second lease. Two holders would both
	// read the database, which is the stampede this exists to prevent.
	second, err := c.GetOrLease(ctx, "lease:exclusive")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if second.Outcome != cache.LeaseWait {
		t.Errorf("the second caller got %v, want LeaseWait", second.Outcome)
	}
	if second.Token != "" {
		t.Error("a waiting caller was handed a token; it would fill as though it held the lease")
	}
}

func TestAHitNeedsNoLease(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	if _, err := c.Fill(ctx, "lease:hit", cache.Entry{RowVersion: 5, FillVersion: 5, Payload: []byte("v")}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	res, err := c.GetOrLease(ctx, "lease:hit")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if res.Outcome != cache.LeaseHit {
		t.Fatalf("Outcome = %v on a present entry, want LeaseHit", res.Outcome)
	}
	if string(res.Entry.Payload) != "v" || res.Entry.RowVersion != 5 {
		t.Errorf("Entry = %+v, want the filled value", res.Entry)
	}
	// Taking a lease on a hit would leave a lease key behind for a fill nobody is going to do,
	// blocking the next genuine miss until it expired.
	if res.Token != "" {
		t.Error("a cache hit was handed a lease token")
	}
}

func TestAHitReturnsEveryField(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	want := cache.Entry{RowVersion: 11, FillVersion: 22, TenantID: 33, Status: 4, Payload: []byte("body")}
	if _, err := c.Fill(ctx, "lease:fields", want); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	// The lease read replaces the plain read on the hot path, so it has to return exactly what the
	// plain read does. A hit that dropped a field would make the answer depend on which code path
	// served it — the same class of bug the conformance suite already caught once.
	res, err := c.GetOrLease(ctx, "lease:fields")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	got := res.Entry
	if got.RowVersion != want.RowVersion || got.FillVersion != want.FillVersion ||
		got.TenantID != want.TenantID || got.Status != want.Status ||
		string(got.Payload) != string(want.Payload) || got.Negative != want.Negative {
		t.Errorf("Entry = %+v, want %+v", got, want)
	}
}

func TestANegativeEntryIsAHitNotAMiss(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	if _, err := c.Fill(ctx, "lease:negative", cache.Entry{RowVersion: 7, FillVersion: 7, Negative: true}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	// "This row does not exist" is a cached FACT. Treating it as a miss would grant a lease and send
	// someone to the database to rediscover an absence already known — which is the whole reason
	// negative caching exists.
	res, err := c.GetOrLease(ctx, "lease:negative")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if res.Outcome != cache.LeaseHit {
		t.Fatalf("Outcome = %v on a negative entry, want LeaseHit", res.Outcome)
	}
	if !res.Entry.Negative {
		t.Error("the entry lost its negative flag")
	}
}

func TestATombstonedEntryGrantsALease(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	if _, err := c.Fill(ctx, "lease:tombstoned", cache.Entry{RowVersion: 5, FillVersion: 5, Payload: []byte("old")}); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if _, err := c.Tombstone(ctx, "lease:tombstoned", 9); err != nil {
		t.Fatalf("Tombstone: %v", err)
	}

	// A tombstone reads as a miss, so it must also grant a lease — otherwise every invalidated hot
	// key becomes a stampede the moment it is invalidated, which is precisely when traffic is
	// highest.
	res, err := c.GetOrLease(ctx, "lease:tombstoned")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if res.Outcome != cache.LeaseGranted {
		t.Errorf("Outcome = %v on a tombstoned entry, want LeaseGranted", res.Outcome)
	}
}

func TestFillingReleasesTheLease(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	granted, err := c.GetOrLease(ctx, "lease:release")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if granted.Outcome != cache.LeaseGranted {
		t.Fatalf("precondition: got %v, want LeaseGranted", granted.Outcome)
	}

	// Filling is what the lease was for, so completing the fill must hand it back in the same
	// operation. Leaving it to expire would stall the next miss for the rest of the lease interval
	// after the work it was protecting had already finished.
	if _, err := c.FillWithLease(ctx, "lease:release", cache.Entry{RowVersion: 1, FillVersion: 1, Payload: []byte("v")}, granted.Token); err != nil {
		t.Fatalf("FillWithLease: %v", err)
	}

	if held, err := c.LeaseHeldForTest(ctx, "lease:release"); err != nil {
		t.Fatalf("LeaseHeldForTest: %v", err)
	} else if held {
		t.Error("the lease is still held after the fill that it existed to protect completed")
	}
}

func TestAStaleTokenCannotReleaseSomeoneElsesLease(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	granted, err := c.GetOrLease(ctx, "lease:steal")
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}

	// A holder whose lease expired, and whose key was then leased by someone else, must not be able
	// to release the NEW holder's lease when its slow fill finally lands. That would admit a second
	// filler and reopen the stampede the lease just closed.
	if _, err := c.FillWithLease(ctx, "lease:steal", cache.Entry{RowVersion: 1, FillVersion: 1}, "not-the-token"); err != nil {
		t.Fatalf("FillWithLease: %v", err)
	}

	held, err := c.LeaseHeldForTest(ctx, "lease:steal")
	if err != nil {
		t.Fatalf("LeaseHeldForTest: %v", err)
	}
	if !held {
		t.Error("a stale token released a lease it does not own")
	}
	_ = granted
}

func TestALeaseExpiresSoADeadHolderCannotStallAKey(t *testing.T) {
	ctx := context.Background()
	// A short lease so the test does not sit through the production interval. newClient is called
	// first purely to guarantee the shared container is up and to register its cleanup.
	newClient(ctx, t)

	c, err := cache.New(ctx, cache.Options{
		Addresses: []string{addr},
		TTL:       time.Hour,
		LeaseTTL:  300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if _, err := c.GetOrLease(ctx, "lease:expiry"); err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}

	// The holder dies here: it never fills and never releases. Without an expiry this key would be
	// unfillable for the lifetime of the process, and every reader of it would be permanently
	// downgraded to a database read.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		res, err := c.GetOrLease(ctx, "lease:expiry")
		if err != nil {
			t.Fatalf("GetOrLease: %v", err)
		}
		if res.Outcome == cache.LeaseGranted {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("the key never became leasable again; a holder that died mid-fill stalls it forever")
}

func TestOnlyOneCallerFillsUnderAStampede(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	// The headline claim, at the cache layer: one lease per key, no matter how many callers arrive
	// at once. The engine's wait/retry protocol is what turns this into bounded ORIGIN load; this
	// test pins the primitive underneath it.
	const callers = 200
	var (
		granted atomic.Int64
		waited  atomic.Int64
		hits    atomic.Int64
		wg      sync.WaitGroup
		start   = make(chan struct{})
	)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := c.GetOrLease(ctx, "lease:stampede")
			if err != nil {
				t.Errorf("GetOrLease: %v", err)
				return
			}
			switch res.Outcome {
			case cache.LeaseGranted:
				granted.Add(1)
			case cache.LeaseWait:
				waited.Add(1)
			case cache.LeaseHit:
				hits.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := granted.Load(); got != 1 {
		t.Errorf("%d of %d concurrent callers were granted a lease, want exactly 1; origin load is "+
			"not bounded", got, callers)
	}
	if waited.Load() == 0 {
		t.Error("no caller was told to wait; the others cannot have been held back")
	}
}
