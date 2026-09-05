//go:build e2e

package e2e_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// TestTopologiesBehaveIdentically is amendment D1's acceptance test.
//
// The sidecar is the default topology and reaches the engine over a Unix socket, while the service
// tier uses TCP (ADR 0004). "Topology is a deployment choice, not a product variant" is only true
// if the two are observably the same system — so every operation is exercised over both sockets and
// the results are compared, rather than the TCP path being tested and the Unix path assumed.
func TestTopologiesBehaveIdentically(t *testing.T) {
	ctx := context.Background()
	sock := harness.SocketPath(t)
	cluster := harness.Start(ctx, t, "tcp://127.0.0.1:0", "unix://"+sock)

	if len(cluster.Addrs) != 2 {
		t.Fatalf("cluster bound %d listeners, want 2", len(cluster.Addrs))
	}

	for _, addr := range cluster.Addrs {
		t.Run(addr.Network(), func(t *testing.T) {
			client := cluster.Client(t, addr)
			runContractSuite(ctx, t, client, addr)
		})
	}
}

func runContractSuite(ctx context.Context, t *testing.T, c cachetv1.CacheServiceClient, addr net.Addr) {
	t.Helper()

	// Distinct ids per transport so the two subtests cannot interfere; they share one database.
	base := uint64(9_000_000)
	if addr.Network() == "unix" {
		base = 9_100_000
	}
	key := func(offset uint64) string { return "entities:" + itoa(base+offset) }

	t.Run("handshake", func(t *testing.T) {
		resp, err := c.Handshake(ctx, &cachetv1.HandshakeRequest{ProtocolVersion: "cachet.v1"})
		if err != nil {
			t.Fatalf("Handshake: %v", err)
		}
		if !resp.GetCompatible() {
			t.Errorf("incompatible: %s", resp.GetIncompatibilityReason())
		}
		if len(resp.GetSupportedLevels()) != 4 {
			t.Errorf("server advertises %d levels, want 4", len(resp.GetSupportedLevels()))
		}
	})

	t.Run("handshake rejects a future protocol", func(t *testing.T) {
		// Telling a newer client plainly that it is unsupported is the entire point of negotiating
		// on connect; the alternative is failing later on an unrelated field.
		resp, err := c.Handshake(ctx, &cachetv1.HandshakeRequest{ProtocolVersion: "cachet.v9"})
		if err != nil {
			t.Fatalf("Handshake: %v", err)
		}
		if resp.GetCompatible() {
			t.Error("server claimed compatibility with cachet.v9")
		}
		if resp.GetIncompatibilityReason() == "" {
			t.Error("incompatibility was reported without a reason")
		}
	})

	t.Run("put then get returns the row", func(t *testing.T) {
		put, err := c.Put(ctx, &cachetv1.PutRequest{
			Key:    key(1),
			Record: &cachetv1.Record{TenantId: 3, Status: 1, Payload: []byte("hello")},
		})
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if put.GetMeta().GetVersion() == 0 {
			t.Error("Put returned version 0; the watermark would never advance")
		}

		got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key(1), Session: put.GetSession()})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !got.GetFound() {
			t.Fatal("Get did not find the row that Put had just written")
		}
		if string(got.GetRecord().GetPayload()) != "hello" {
			t.Errorf("payload = %q, want \"hello\"", got.GetRecord().GetPayload())
		}
		// Read-own-writes, observed end to end: the value read is at least the version written.
		if got.GetRecord().GetVersion() < put.GetMeta().GetVersion() {
			t.Errorf("read version %d precedes the write's %d",
				got.GetRecord().GetVersion(), put.GetMeta().GetVersion())
		}
	})

	t.Run("session watermark advances and is returned", func(t *testing.T) {
		put, err := c.Put(ctx, &cachetv1.PutRequest{
			Key:    key(2),
			Record: &cachetv1.Record{TenantId: 1, Payload: []byte("x")},
		})
		if err != nil {
			t.Fatalf("Put: %v", err)
		}

		// The token is client-held; the server is stateless with respect to sessions, which is what
		// lets the guarantee survive a reconnect or an engine failover (CONSISTENCY.md §4).
		wm := put.GetSession().GetWatermarks()
		if len(wm) == 0 {
			t.Fatal("Put returned an empty session token; read-own-writes would be impossible")
		}
		var found bool
		for _, v := range wm {
			if v >= put.GetMeta().GetVersion() {
				found = true
			}
		}
		if !found {
			t.Errorf("no watermark reached the committed version %d: %v", put.GetMeta().GetVersion(), wm)
		}
	})

	t.Run("missing row is found=false, not an error", func(t *testing.T) {
		got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key(999)})
		if err != nil {
			t.Fatalf("Get on a missing row returned an error: %v", err)
		}
		// Absence is a cacheable fact, so it has to be an answer rather than a failure.
		if got.GetFound() {
			t.Error("Get reported found for a row that was never written")
		}
	})

	t.Run("delete removes the row", func(t *testing.T) {
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: key(3), Record: &cachetv1.Record{TenantId: 1, Payload: []byte("doomed")},
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		del, err := c.Delete(ctx, &cachetv1.DeleteRequest{Key: key(3)})
		if err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if !del.GetExisted() {
			t.Error("Delete reported the row did not exist")
		}
		if del.GetMeta().GetVersion() == 0 {
			t.Error("Delete returned version 0; the invalidation would have nothing to stamp")
		}

		got, err := c.Get(ctx, &cachetv1.GetRequest{Key: key(3), Session: del.GetSession()})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.GetFound() {
			t.Error("the row survived its own delete")
		}
	})

	t.Run("deleting an absent row is not an error", func(t *testing.T) {
		del, err := c.Delete(ctx, &cachetv1.DeleteRequest{Key: key(998)})
		if err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if del.GetExisted() {
			t.Error("Delete reported existed for a row that was never written")
		}
	})

	t.Run("batch get spans shards and omits missing rows", func(t *testing.T) {
		keys := []string{key(10), key(11), key(12)}
		for _, k := range keys {
			if _, err := c.Put(ctx, &cachetv1.PutRequest{
				Key: k, Record: &cachetv1.Record{TenantId: 2, Payload: []byte("batch")},
			}); err != nil {
				t.Fatalf("Put %s: %v", k, err)
			}
		}

		resp, err := c.BatchGet(ctx, &cachetv1.BatchGetRequest{Keys: append(keys, key(997))})
		if err != nil {
			t.Fatalf("BatchGet: %v", err)
		}
		if len(resp.GetRecords()) != 3 {
			t.Fatalf("BatchGet returned %d records, want 3", len(resp.GetRecords()))
		}
		if _, present := resp.GetRecords()[key(997)]; present {
			t.Error("BatchGet returned an entry for a row that does not exist")
		}
	})

	t.Run("every consistency level is served", func(t *testing.T) {
		if _, err := c.Put(ctx, &cachetv1.PutRequest{
			Key: key(20), Record: &cachetv1.Record{TenantId: 1, Payload: []byte("levels")},
		}); err != nil {
			t.Fatalf("Put: %v", err)
		}

		for _, lv := range []cachetv1.ConsistencyLevel{
			cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG,
			cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION,
			cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL,
		} {
			resp, err := c.Get(ctx, &cachetv1.GetRequest{Key: key(20), Level: lv})
			if err != nil {
				t.Errorf("Get at %v: %v", lv, err)
				continue
			}
			if resp.GetMeta().GetLevelServed() != lv {
				t.Errorf("asked for %v, served %v", lv, resp.GetMeta().GetLevelServed())
			}
			// Phase 0 has no cache, so nothing may claim a hit. When the cache lands, this
			// assertion is what proves the flag started reporting reality.
			if resp.GetMeta().GetCacheHit() {
				t.Errorf("cache_hit is true at %v, but Phase 0 has no cache", lv)
			}
		}
	})

	t.Run("bounded without a bound is rejected", func(t *testing.T) {
		// A bound the caller did not choose is a guarantee the caller cannot rely on, so the
		// request is refused rather than given a default (CONSISTENCY.md §3.3).
		_, err := c.Get(ctx, &cachetv1.GetRequest{
			Key:   key(20),
			Level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED,
		})
		if err == nil {
			t.Error("BOUNDED with no staleness bound was accepted")
		}
	})

	t.Run("malformed keys are rejected", func(t *testing.T) {
		for _, bad := range []string{"", "entities", "entities:abc", "other:1"} {
			if _, err := c.Get(ctx, &cachetv1.GetRequest{Key: bad}); err == nil {
				t.Errorf("Get(%q) was accepted", bad)
			}
		}
	})
}

func durationProto(d time.Duration) *durationpb.Duration { return durationpb.New(d) }

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
