//go:build e2e

package conformance_test

import (
	"context"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
)

// CONSISTENCY.md §5. A conditional write does not name its affected keys; the engine resolves them
// exactly inside the transaction. Past max_affected_keys that resolution is abandoned as too
// expensive and the write relies on CDC instead.
//
// What that costs is stated precisely, and these tests pin each half:
//
//   - The writer's OWN session guarantee survives, because the watermark still advances. This falls
//     out of watermarking on the fill version rather than the row version — the second payoff of a
//     decision made on paper in T2, before any of this code existed.
//   - OTHER sessions' reads of the affected keys become BOUNDED(cdc_lag_bound) until Flux catches up,
//     instead of being invalidated before the ack.
//
// The reason the degraded flag matters more than the degradation: a silent downgrade of a stated
// guarantee is exactly the failure this project exists to eliminate.

// updateWhere issues a conditional write.
func updateWhere(t *testing.T, e *env, tenant uint32, match, set uint32, tok *cachetv1.SessionToken) *cachetv1.UpdateWhereResponse {
	t.Helper()

	resp, err := e.client.UpdateWhere(context.Background(), &cachetv1.UpdateWhereRequest{
		TenantId:    tenant,
		MatchStatus: match,
		SetStatus:   set,
		Session:     tok,
	})
	if err != nil {
		t.Fatalf("UpdateWhere(tenant=%d): %v", tenant, err)
	}
	return resp
}

// assertEveryShardHolds fails unless every shard received at least min of the given keys.
//
// The affected-key budget is PER SHARD, because each shard resolves and commits in its own
// transaction and the cost being capped — holding a transaction open across row locks — is a
// per-transaction cost. That makes a test's outcome depend on how the ring happened to distribute
// its keys, so the distribution is asserted rather than assumed. Without this, a degraded-write test
// passes or fails according to which shard got the extra row, which is the definition of a flake.
func assertEveryShardHolds(t *testing.T, e *env, keys []string, min int) {
	t.Helper()

	perShard := map[string]int{}
	for _, k := range keys {
		shard, err := e.cluster.Router.ShardFor(k)
		if err != nil {
			t.Fatalf("route %s: %v", k, err)
		}
		perShard[string(shard)]++
	}
	for _, shard := range e.cluster.Router.Shards() {
		if got := perShard[string(shard)]; got < min {
			t.Fatalf("shard %s holds only %d of the %d seeded rows, need at least %d for this test "+
				"to provoke degradation on every shard: %v", shard, got, len(keys), min, perShard)
		}
	}
}

// seedTenantRows writes n rows into a tenant and warms each into the cache.
func seedTenantRows(t *testing.T, e *env, tenant uint32, n int) []string {
	t.Helper()

	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key := e.key()
		if _, err := e.client.Put(context.Background(), &cachetv1.PutRequest{
			Key:    key,
			Record: &cachetv1.Record{TenantId: tenant, Status: 1, Payload: []byte("v1")},
		}); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
		warm(t, e, key)
		keys = append(keys, key)
	}
	return keys
}

// TestASmallPredicateInvalidatesExactly is the non-degraded baseline.
//
// Without it, every assertion about degradation would be untethered: a system that degraded on
// everything would pass the degraded tests and fail nobody's notice.
func TestASmallPredicateInvalidatesExactly(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true})
	const tenant = 61000

	keys := seedTenantRows(t, e, tenant, 3)
	resp := updateWhere(t, e, tenant, 1, 2, nil)

	if resp.GetMeta().GetDegraded() {
		t.Fatalf("a 3-row predicate degraded: %s", resp.GetMeta().GetDegradedReason())
	}
	if got := len(resp.GetAffectedKeys()); got != len(keys) {
		t.Errorf("AffectedKeys has %d entries, want %d", got, len(keys))
	}

	// Every affected key must be invalidated before the ack, so a DIFFERENT session sees the change
	// immediately.
	for _, key := range keys {
		resp := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION}, nil)
		if got := resp.GetRecord().GetStatus(); got != 2 {
			t.Errorf("%s still reads status %d from another session; exact invalidation did not run", key, got)
		}
	}
}

// TestDegradedFlagIsSurfacedToCaller: the caller must be TOLD.
//
// Ignoring the flag is the caller's decision, but it has to be a visible one. This is the assertion
// that keeps a degraded write from looking identical to an exact one.
func TestDegradedFlagIsSurfacedToCaller(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true, maxAffectedKeys: 1})
	const tenant = 61100

	keys := seedTenantRows(t, e, tenant, 24)
	assertEveryShardHolds(t, e, keys, 2)

	resp := updateWhere(t, e, tenant, 1, 2, nil)

	meta := resp.GetMeta()
	if !meta.GetDegraded() {
		t.Fatal("a 24-row predicate under a 1-key-per-shard budget did not report degraded")
	}
	if meta.GetDegradedReason() == "" {
		t.Error("degraded was set with no reason; an operator cannot act on a bare boolean")
	}
	if meta.GetEffectiveStalenessBound().AsDuration() <= 0 {
		t.Error("a degraded write named no effective staleness bound; §5 requires BOUNDED(cdc_lag_bound)")
	}
	if n := len(resp.GetAffectedKeys()); n != 0 {
		t.Errorf("a degraded write returned %d affected keys; a partial list is worse than none, "+
			"because the caller cannot tell which rows were left to CDC", n)
	}
	if meta.GetVersion() == 0 {
		t.Error("a degraded write reported no version; the writer's own guarantee depends on it")
	}
}

// TestLargePredicateDegradesOthersNotSelf is the sharpest claim in §5.
//
// The writer keeps read-own-writes even though nothing invalidated the entry, because its watermark
// advanced past the entry's fill version and the freshness check rejects it. Other sessions, holding
// no such watermark, go on reading the stale entry until CDC catches up.
func TestLargePredicateDegradesOthersNotSelf(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true, maxAffectedKeys: 1})
	const tenant = 61200

	keys := seedTenantRows(t, e, tenant, 24)
	assertEveryShardHolds(t, e, keys, 2)

	resp := updateWhere(t, e, tenant, 1, 2, nil)
	if !resp.GetMeta().GetDegraded() {
		t.Fatal("precondition: expected this write to degrade")
	}

	// The writer, carrying the token it was handed. Every key must reflect the write.
	for _, key := range keys {
		r := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION}, resp.GetSession())
		if got := r.GetRecord().GetStatus(); got != 2 {
			t.Errorf("the WRITER reads status %d for %s after its own degraded write; read-own-writes "+
				"was lost, which §5 says must survive degradation", got, key)
		}
	}

	// Another session, holding no watermark. It is permitted to be stale here — that is what
	// degraded means — so this is recorded rather than asserted. Asserting staleness would forbid
	// CDC from having already caught up, which is a legitimate outcome.
	stale := 0
	for _, key := range keys {
		r := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION}, nil)
		if r.GetRecord().GetStatus() != 2 {
			stale++
		}
	}
	t.Logf("after a degraded write, %d/%d keys were still stale for a session holding no watermark", stale, len(keys))
}

// TestDegradedConvergesWithinCDCLagBound: degradation is bounded, not permanent.
//
// Without CDC running there is nothing to converge, so this test runs the tailer. The bound is what
// turns "we gave up on exact invalidation" from an outage into a stated, finite weakening.
func TestDegradedConvergesWithinCDCLagBound(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true, maxAffectedKeys: 1})
	const tenant = 61300

	keys := seedTenantRows(t, e, tenant, 24)
	assertEveryShardHolds(t, e, keys, 2)
	startTailers(t, e)

	resp := updateWhere(t, e, tenant, 1, 2, nil)
	if !resp.GetMeta().GetDegraded() {
		t.Fatal("precondition: expected this write to degrade")
	}

	bound := resp.GetMeta().GetEffectiveStalenessBound().AsDuration()
	if bound <= 0 {
		t.Fatal("no effective staleness bound was reported")
	}

	// Generous headroom over the stated bound: the claim under test is that it converges, and a
	// tight deadline would turn a slow CI machine into a consistency failure.
	deadline := time.Now().Add(bound + 30*time.Second)
	for {
		remaining := 0
		for _, key := range keys {
			r := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL}, nil)
			if r.GetRecord().GetStatus() != 2 {
				remaining++
			}
		}
		if remaining == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d/%d keys are still stale %s after a degraded write; CDC did not converge "+
				"within the bound the response promised (%s)", remaining, len(keys), bound+30*time.Second, bound)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestDegradedWriteStillCommits: degraded describes the invalidation, never the durability.
func TestDegradedWriteStillCommits(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true, maxAffectedKeys: 1})
	const tenant = 61400

	keys := seedTenantRows(t, e, tenant, 24)
	assertEveryShardHolds(t, e, keys, 2)

	resp := updateWhere(t, e, tenant, 1, 2, nil)
	if !resp.GetMeta().GetDegraded() {
		t.Fatal("precondition: expected this write to degrade")
	}
	if resp.GetMatched() != uint64(len(keys)) {
		t.Errorf("Matched = %d, want %d; the blast radius must be reported even when the keys are not",
			resp.GetMatched(), len(keys))
	}

	// STRONG bypasses the cache entirely, so this reads the database and nothing else.
	for _, key := range keys {
		r := get(t, e, key, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG}, nil)
		if got := r.GetRecord().GetStatus(); got != 2 {
			t.Errorf("%s has status %d in the DATABASE after a degraded write, want 2; the write did "+
				"not commit, which degradation must never mean", key, got)
		}
	}
}

// TestUpdateWhereAdvancesTheWatermark: a conditional write is a write, and must move the session on
// every shard it touched.
func TestUpdateWhereAdvancesTheWatermark(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true})
	const tenant = 61500

	seedTenantRows(t, e, tenant, 4)
	resp := updateWhere(t, e, tenant, 1, 2, nil)

	if len(resp.GetSession().GetWatermarks()) == 0 {
		t.Fatal("a conditional write returned an empty watermark; the writer would have no basis " +
			"for read-own-writes")
	}
	for shard, wm := range resp.GetSession().GetWatermarks() {
		if wm == 0 {
			t.Errorf("shard %s has a zero watermark", shard)
		}
	}
}

// TestUpdateWhereMatchingNothingIsNotAnError: an empty predicate is a normal outcome.
func TestUpdateWhereMatchingNothingIsNotAnError(t *testing.T) {
	e := newEnv(t, config{synchronousInvalidation: true})

	resp := updateWhere(t, e, 61600, 1, 2, nil)
	if resp.GetMatched() != 0 {
		t.Errorf("Matched = %d, want 0", resp.GetMatched())
	}
	if resp.GetMeta().GetDegraded() {
		t.Error("a predicate matching nothing reported degraded")
	}
	if n := len(resp.GetAffectedKeys()); n != 0 {
		t.Errorf("AffectedKeys has %d entries, want none", n)
	}
}
