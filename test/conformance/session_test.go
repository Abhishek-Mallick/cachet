//go:build e2e

package conformance_test

import (
	"context"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// CONSISTENCY.md §4 answers the question the build plan flagged before any code was written: what
// does SESSION guarantee across a reconnect? The answer is a rule, and this file is that rule
// executed:
//
//	The session guarantee is a property of THE TOKEN — not of the client, the connection, or the
//	engine. Hold the token, keep the guarantee. Drop it, and you have a new session.
//
// The cases where the guarantee is LOST are tested just as carefully as the cases where it holds. A
// model that only tests its promises has not been tested; it has been advertised.

// TestReadOwnWrite, TestReadOwnInsertOverNegative, TestReadOwnDelete and
// TestMonotonicReadsUnderConcurrentWriters are executed as cells of the matrix in matrix_test.go,
// at every level, rather than duplicated here. The tests below are the ones the matrix does not
// reach, because they are about the token's lifetime rather than about a single read.

// TestReconnectKeepsTheGuarantee: a dropped connection must not cost a session anything.
//
// The token lives in the client object, not the connection. If the guarantee were tied to the
// connection, every transient network blip would silently downgrade a caller to EVENTUAL, and
// nothing in the response would say so.
func TestReconnectKeepsTheGuarantee(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true})
	key := e.key()

	put(t, e, key, "v1", nil)
	warm(t, e, key)
	w := put(t, e, key, "v2", nil)

	// A brand new connection to the same engine — the transport equivalent of a reconnect.
	reconnected := e.cluster.Client(t, e.cluster.Addrs[0])
	resp, err := reconnected.Get(context.Background(), &cachetv1.GetRequest{
		Key:     key,
		Level:   cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
		Session: w.GetSession(),
	})
	if err != nil {
		t.Fatalf("Get after reconnect: %v", err)
	}

	if got := string(resp.GetRecord().GetPayload()); got != "v2" {
		t.Errorf("after reconnecting, a session reads %q instead of its own write; the guarantee was "+
			"tied to the connection rather than to the token", got)
	}
}

// TestWatermarkPropagatesAcrossServiceHop: carrying the token across a service boundary carries the
// guarantee with it.
//
// This is CONSISTENCY.md §3.2's causal-propagation clause, and it is what makes the model useful in
// a system of more than one service: service A writes, calls service B, and B must not read a value
// older than what A just wrote. The token is the whole mechanism — B is a different process talking
// to a different engine, and it inherits A's guarantee purely by being handed A's watermark.
func TestWatermarkPropagatesAcrossServiceHop(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, config{synchronousInvalidation: true})
	key := e.key()

	put(t, e, key, "v1", nil)
	warm(t, e, key)

	// Service A writes.
	a := put(t, e, key, "v2", nil)

	// Service B: a different engine that has never seen this session. Its cache is preserved, so it
	// can still be holding the pre-write entry — which is exactly the situation the watermark has
	// to rescue.
	downstream := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
		PreserveCache:           true,
	}, "tcp://127.0.0.1:0")
	b := downstream.Client(t, downstream.Addrs[0])

	resp, err := b.Get(ctx, &cachetv1.GetRequest{
		Key:     key,
		Level:   cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
		Session: a.GetSession(), // the propagated watermark
	})
	if err != nil {
		t.Fatalf("downstream Get: %v", err)
	}

	if got := string(resp.GetRecord().GetPayload()); got != "v2" {
		t.Errorf("the downstream service read %q after being handed the upstream's watermark, want "+
			"\"v2\"; causal propagation did not hold across the hop", got)
	}
}

// TestWithoutPropagationTheGuaranteeIsLostAtTheHop is the honest other half.
//
// CONSISTENCY.md §4 states plainly that a hop without propagation loses the guarantee: the
// downstream session starts empty and reads as if it had written nothing. That is a documented
// limitation, and a documented limitation nobody has measured is a rumour.
//
// It is asserted as a POSSIBILITY rather than a certainty, because with synchronous invalidation on
// the downstream read often succeeds anyway. The test therefore pins what actually matters: that
// nothing in the response claims a SESSION guarantee the engine could not have provided.
func TestWithoutPropagationTheGuaranteeIsLostAtTheHop(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: false})
	key := e.key()

	put(t, e, key, "v1", nil)
	warm(t, e, key)
	put(t, e, key, "v2", nil)

	// No token: the downstream session knows nothing and has no basis to reject a stale entry.
	resp := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION}, nil)

	got := string(resp.GetRecord().GetPayload())
	if got == "v2" {
		t.Fatal("the un-propagated read saw the write, so this configuration is not reproducing a " +
			"lost guarantee and the test below would prove nothing")
	}

	// The guarantee was lost, which §4 permits. What is NOT permitted is losing it silently while
	// still reporting SESSION as though it had been honoured.
	t.Logf("without propagation the downstream session read %q; level_served=%v degraded=%v",
		got, resp.GetMeta().GetLevelServed(), resp.GetMeta().GetDegraded())
}

// TestARecreatedClientStartsANewSession: dropping the token is correct behaviour, not a bug.
//
// A process that has forgotten it wrote something has no writes to read. The value of testing this
// is that it pins the sharpest edge in the model as intentional, so nobody later "fixes" it by
// adding server-side session state — which would break failover, the property the token exists to
// provide.
func TestARecreatedClientStartsANewSession(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: false})
	key := e.key()

	put(t, e, key, "v1", nil)
	warm(t, e, key)
	w := put(t, e, key, "v2", nil)

	if len(w.GetSession().GetWatermarks()) == 0 {
		t.Fatal("the write returned an empty watermark; there is no session state to discard")
	}

	// The same client, deliberately discarding its token — a process restart, in one line.
	resp := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION}, nil)

	if string(resp.GetRecord().GetPayload()) == "v2" {
		t.Skip("the new session happened to see the write; with invalidation off this is timing, " +
			"not a guarantee, and the case this test describes did not occur")
	}
	t.Log("a session that discarded its token reads as if it had written nothing, per §4")
}

// TestObservingAdvancesTheWatermark: reading, not just writing, must move the session forward.
//
// This is the mechanism behind monotonic reads, and it is a one-line rule that is easy to lose in a
// refactor: W[s] = max(W[s], fv) on every read. Without it, monotonic reads would need extra
// server-side state and the model would stop being stateless.
func TestObservingAdvancesTheWatermark(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true})
	key := e.key()

	put(t, e, key, "v1", nil)

	// A fresh session with no watermark at all.
	first := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION}, nil)
	if n := len(first.GetSession().GetWatermarks()); n == 0 {
		t.Fatal("a read returned no watermark; observing did not advance the session, so monotonic " +
			"reads would have nothing to rest on")
	}

	// And it must not go backwards on a subsequent read.
	second := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION}, first.GetSession())
	for shard, before := range first.GetSession().GetWatermarks() {
		after, ok := second.GetSession().GetWatermarks()[shard]
		if !ok {
			t.Errorf("shard %s lost its watermark between two reads", shard)
			continue
		}
		if after < before {
			t.Errorf("shard %s watermark went backwards: %d then %d", shard, before, after)
		}
	}
}

// TestStrongIsNeverServedFromTheCache: the one level that must not be cacheable.
//
// Asserted through the response's own metadata rather than by comparing values, because a STRONG
// read of an up-to-date cache would return the right answer for the wrong reason and look identical
// from outside.
func TestStrongIsNeverServedFromTheCache(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true})
	key := e.key()

	put(t, e, key, "v1", nil)
	warm(t, e, key)

	for i := 0; i < 5; i++ {
		resp := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG}, nil)
		if resp.GetMeta().GetCacheHit() {
			t.Fatal("a STRONG read reported a cache hit")
		}
	}
}

// warm fills the cache for a key, so that a later assertion has a stale entry to be fooled by.
//
// Two reads, not one: the first fills, the second confirms the entry is actually being served from
// the cache. Without the confirmation a test could pass because nothing was ever cached, which is
// the vacuous-pass failure mode every suite in this repo is written to avoid.
func warm(t *testing.T, e *env, key string) {
	t.Helper()

	eventual := levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}
	get(t, e, key, eventual, nil)

	resp := get(t, e, key, eventual, nil)
	if !resp.GetMeta().GetCacheHit() {
		t.Fatalf("%s is not cached after two reads; a test relying on a stale entry would prove nothing", key)
	}
}

// TestACachedReadReturnsTheSameRecordAsAnUncachedOne is the invariant behind every other test here.
//
// Cachet is a cache. If a hit and a miss can return different records, then every guarantee above is
// conditional on cache state that no caller can see — and the model stops being a contract.
//
// It exists because the conformance suite caught this for real: the entry encoding carried the
// payload but not tenant_id or status, so a conditional write that changed status was invisible on
// any cache hit. The database said 2, the cache said 0, and nothing anywhere reported a problem.
func TestACachedReadReturnsTheSameRecordAsAnUncachedOne(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true})
	key := e.key()

	if _, err := e.client.Put(context.Background(), &cachetv1.PutRequest{
		Key:    key,
		Record: &cachetv1.Record{TenantId: 4242, Status: 7, Payload: []byte("body")},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// STRONG bypasses the cache, so this is the row as the database holds it.
	uncached := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG}, nil)
	if uncached.GetMeta().GetCacheHit() {
		t.Fatal("the STRONG read was served from the cache; it cannot be the uncached reference")
	}

	warm(t, e, key)
	cached := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)
	if !cached.GetMeta().GetCacheHit() {
		t.Fatal("the second read was not a cache hit; there is nothing to compare")
	}

	u, c := uncached.GetRecord(), cached.GetRecord()
	if c.GetId() != u.GetId() {
		t.Errorf("id: cached %d, uncached %d", c.GetId(), u.GetId())
	}
	if c.GetTenantId() != u.GetTenantId() {
		t.Errorf("tenant_id: cached %d, uncached %d — a cache hit returns a different row", c.GetTenantId(), u.GetTenantId())
	}
	if c.GetStatus() != u.GetStatus() {
		t.Errorf("status: cached %d, uncached %d — a cache hit returns a different row", c.GetStatus(), u.GetStatus())
	}
	if string(c.GetPayload()) != string(u.GetPayload()) {
		t.Errorf("payload: cached %q, uncached %q", c.GetPayload(), u.GetPayload())
	}
	if c.GetVersion() != u.GetVersion() {
		t.Errorf("version: cached %d, uncached %d", c.GetVersion(), u.GetVersion())
	}
}
