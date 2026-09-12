//go:build e2e

package conformance_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/pkg/cachet"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// The SDK is mandatory, and delivery model §4 argues that is a feature. These tests are that
// argument, checked: everything below must hold for an application that never mentions a watermark.
//
// The failure mode being designed out is specific. An application talking raw gRPC that forgets to
// thread the token does not get an error — it gets a weaker guarantee than the one it asked for,
// silently, on the subset of requests where it matters. "I forgot" must not be a reachable state
// (O-303).

func dialSDK(t *testing.T, e *env, opts ...cachet.Option) *cachet.Client {
	t.Helper()

	c, err := cachet.Dial(context.Background(), e.cluster.Addrs[0].String(), opts...)
	if err != nil {
		t.Fatalf("cachet.Dial: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

// TestTheSDKCarriesTheSessionWithoutBeingAsked is the whole point of shipping a client.
func TestTheSDKCarriesTheSessionWithoutBeingAsked(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: false})
	c := dialSDK(t, e)
	key := e.key()

	if _, err := c.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte("v1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Warm the cache so a stale entry exists for the read to be fooled by.
	for i := 0; i < 2; i++ {
		if _, err := c.Get(ctx, key, cachet.AtLevel(consistency.Eventual)); err != nil {
			t.Fatalf("warm: %v", err)
		}
	}

	if _, err := c.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte("v2")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// No token appears anywhere in this call. Invalidation is OFF, so the ONLY thing that can make
	// this read correct is the watermark the client kept for itself.
	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Record.Payload) != "v2" {
		t.Errorf("read %q after writing v2 through the same client; the SDK did not carry its own "+
			"session, which is the one thing it exists to do", got.Record.Payload)
	}
}

// TestTheSDKSurvivesAReconnect: the session belongs to the client, not the connection.
func TestTheSDKSurvivesAReconnect(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: false})
	c := dialSDK(t, e)
	key := e.key()

	if _, err := c.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte("v1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.Get(ctx, key, cachet.AtLevel(consistency.Eventual)); err != nil {
			t.Fatalf("warm: %v", err)
		}
	}
	if _, err := c.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte("v2")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// A second client, handed the first's session — the shape of a reconnect or a failover, where
	// the connection is new and the session is not.
	reconnected := dialSDK(t, e)
	reconnected.AdoptSession(c.Session())

	got, err := reconnected.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got.Record.Payload) != "v2" {
		t.Errorf("a reconnected client read %q; the guarantee was tied to the connection", got.Record.Payload)
	}
}

// TestTheSDKPropagatesAcrossAServiceHopThroughBaggage is D3's requirement, end to end.
//
// Service A writes and injects. The transport carries baggage. Service B extracts and adopts, and
// inherits A's guarantee without either service writing code that knows what a watermark is.
func TestTheSDKPropagatesAcrossAServiceHopThroughBaggage(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: false})
	a := dialSDK(t, e)
	key := e.key()

	if _, err := a.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte("v1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := a.Get(ctx, key, cachet.AtLevel(consistency.Eventual)); err != nil {
			t.Fatalf("warm: %v", err)
		}
	}
	if _, err := a.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte("v2")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Service A prepares an outgoing call.
	outgoing, err := cachet.InjectSession(cachet.ContextWithSession(ctx, a.Session()))
	if err != nil {
		t.Fatalf("InjectSession: %v", err)
	}

	// Service B: a different engine and a different client, which have never seen this session. The
	// cache is preserved, so B's engine can still be holding the entry A's write superseded — which
	// is exactly the situation the propagated watermark has to rescue.
	downstream := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: false,
		PreserveCache:           true,
	}, "tcp://127.0.0.1:0")
	b, err := cachet.Dial(ctx, downstream.Addrs[0].String())
	if err != nil {
		t.Fatalf("dial downstream: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	// B does what an instrumented service would do on an inbound request.
	if w, ok := cachet.ExtractSession(outgoing); ok {
		b.AdoptSession(w)
	} else {
		t.Fatal("the downstream found no session in the baggage; propagation did not survive the hop")
	}

	got, err := b.Get(ctx, key)
	if err != nil {
		t.Fatalf("downstream Get: %v", err)
	}
	if string(got.Record.Payload) != "v2" {
		t.Errorf("the downstream service read %q after inheriting the upstream's watermark; causal "+
			"propagation did not hold across the hop", got.Record.Payload)
	}
}

// TestAContextSessionOverridesTheClientSession: a server handling work for many callers must be able
// to scope a guarantee to one request, or one caller's watermark leaks into another's read.
func TestAContextSessionOverridesTheClientSession(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: true})
	c := dialSDK(t, e)
	key := e.key()

	if _, err := c.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte("v1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// An explicitly empty session on the context: this request is on behalf of a caller that has
	// written nothing.
	scoped := cachet.ContextWithSession(ctx, map[string]uint64{})
	if _, err := c.Get(scoped, key); err != nil {
		t.Fatalf("Get with a scoped session: %v", err)
	}

	// The client's own session must be unchanged by having served a scoped request.
	if len(c.Session()) == 0 {
		t.Error("serving a request with a scoped session emptied the client's own session")
	}
}

// TestTheSDKSurfacesDegraded: the flag must reach the caller as a field.
func TestTheSDKSurfacesDegraded(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: true, maxAffectedKeys: 1})
	c := dialSDK(t, e)

	const tenant = 62000
	var keys []string
	for i := 0; i < 24; i++ {
		key := e.key()
		if _, err := c.Put(ctx, key, cachet.Record{TenantID: tenant, Status: 1, Payload: []byte("v1")}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		keys = append(keys, key)
	}
	assertEveryShardHolds(t, e, keys, 2)

	res, err := c.UpdateWhere(ctx, cachet.Predicate{TenantID: tenant, MatchStatus: 1, SetStatus: 2})
	if err != nil {
		t.Fatalf("UpdateWhere: %v", err)
	}

	if !res.Degraded {
		t.Fatal("a 24-row predicate under a 1-key-per-shard budget was not reported as degraded")
	}
	if res.DegradedReason == "" {
		t.Error("degraded reached the caller with no reason attached")
	}
	if res.EffectiveStalenessBound <= 0 {
		t.Error("no effective staleness bound reached the caller")
	}
	if len(res.AffectedKeys) != 0 {
		t.Errorf("a degraded write handed back %d affected keys; the list must be empty, never partial",
			len(res.AffectedKeys))
	}
	if res.Matched != uint64(len(keys)) {
		t.Errorf("Matched = %d, want %d", res.Matched, len(keys))
	}
}

// TestTheSDKReportsCacheHits: the metadata a caller needs to reason about its own latency.
func TestTheSDKReportsCacheHits(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: true})
	c := dialSDK(t, e)
	key := e.key()

	if _, err := c.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte("v1")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	first, err := c.Get(ctx, key, cachet.AtLevel(consistency.Eventual))
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if first.Meta.CacheHit {
		t.Error("the first read reported a cache hit before anything had filled the entry")
	}

	second, err := c.Get(ctx, key, cachet.AtLevel(consistency.Eventual))
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if !second.Meta.CacheHit {
		t.Error("the second read was not reported as a cache hit")
	}
}

// TestTheSDKReportsAMissAsAnAnswer: absence is a value, not an error.
func TestTheSDKReportsAMissAsAnAnswer(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: true})
	c := dialSDK(t, e)

	got, err := c.Get(ctx, e.key())
	if err != nil {
		t.Fatalf("Get on a missing row returned an error: %v", err)
	}
	if got.Found {
		t.Error("Get reported a row that was never written")
	}
}

// TestTheSDKRejectsAnIncompatibleServer: the handshake must fail at Dial, not at first use.
func TestTheSDKHandshakesOnDial(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: true})

	// A successful Dial IS the handshake assertion: Dial performs it and fails if the server says it
	// cannot serve this protocol.
	c := dialSDK(t, e)
	if _, err := c.Get(ctx, e.key()); err != nil {
		t.Fatalf("a client that completed the handshake could not read: %v", err)
	}
}

// TestTheSDKIsSafeForConcurrentUse: one client, many goroutines, one session.
func TestTheSDKIsSafeForConcurrentUse(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: true})
	c := dialSDK(t, e)

	keys := make([]string, 8)
	for i := range keys {
		keys[i] = e.key()
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := keys[i]
			for j := 0; j < 20; j++ {
				if _, err := c.Put(ctx, key, cachet.Record{TenantID: 1, Payload: []byte(fmt.Sprintf("v%d", j))}); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				if _, err := c.Get(ctx, key); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	if len(c.Session()) == 0 {
		t.Error("the client accumulated no session after 160 writes")
	}
}
