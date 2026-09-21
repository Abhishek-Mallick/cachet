//go:build e2e

package e2e_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/proxy"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// The proxy's claim is that an application can use Cachet with no SDK and no code change — so
// these tests drive it with go-sql-driver/mysql, the ordinary MySQL client. Using the proxy's own
// library to test the proxy would prove only that it agrees with itself.

const proxyShardDSN = "root:cachet@tcp(127.0.0.1:3316)/cachet?parseTime=true&interpolateParams=true"

func startProxy(t *testing.T, opaque proxy.OpaqueWritePolicy) (*sql.DB, *harness.Cluster) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// One shard: the proxy speaks to a single upstream, so the engine behind it must route every
	// key to that same database or a read would look for a row the upstream does not have.
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: true,
		Shards:                  []config.Shard{{ID: "shard0", DSN: proxyShardDSN}},
	}, "tcp://127.0.0.1:0")

	srv, err := proxy.New(proxy.Options{
		Listen:           "127.0.0.1:0",
		UpstreamAddr:     "127.0.0.1:3316",
		UpstreamUser:     "root",
		UpstreamPassword: "cachet",
		UpstreamDB:       "cachet",
		User:             "app",
		Password:         "app-secret",
		CachedTable:      "entities",
		Engine:           cluster.Engine,
		Cache:            cluster.Cache,
		OpaqueWrites:     opaque,
	})
	if err != nil {
		t.Fatalf("proxy.New: %v", err)
	}
	go func() { _ = srv.Serve(ctx) }()

	db, err := sql.Open("mysql", fmt.Sprintf("app:app-secret@tcp(%s)/cachet", srv.Addr()))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// interpolateParams is off by default in this driver, so a query with arguments becomes a
	// prepared statement. These tests send literal SQL deliberately: the classifier reasons about
	// statement text, and prepared statements are forwarded rather than cached.
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping through the proxy: %v", err)
	}
	return db, cluster
}

func seedRow(t *testing.T, id uint64, payload string) {
	t.Helper()

	direct, err := sql.Open("mysql", proxyShardDSN)
	if err != nil {
		t.Fatalf("open upstream: %v", err)
	}
	defer func() { _ = direct.Close() }()

	_, err = direct.ExecContext(context.Background(),
		`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, 1, 0, ?, 1)
		 ON DUPLICATE KEY UPDATE status = 0, payload = VALUES(payload), version = 1`, id, payload)
	if err != nil {
		t.Fatalf("seed %d: %v", id, err)
	}
}

// TestAnOrdinaryMySQLClientIsServedFromTheCache is the proxy's whole reason to exist.
func TestAnOrdinaryMySQLClientIsServedFromTheCache(t *testing.T) {
	db, cluster := startProxy(t, proxy.RefuseOpaqueWrites)
	const id = 9_400_001
	seedRow(t, id, "v1")

	q := fmt.Sprintf("SELECT status, payload FROM entities WHERE id = %d", id)

	var status int
	var payload []byte
	if err := db.QueryRowContext(context.Background(), q).Scan(&status, &payload); err != nil {
		t.Fatalf("first read: %v", err)
	}
	if string(payload) != "v1" {
		t.Fatalf("first read returned %q, want v1", payload)
	}

	before := cluster.CacheOpsForTest("get", "hit")
	if err := db.QueryRowContext(context.Background(), q).Scan(&status, &payload); err != nil {
		t.Fatalf("second read: %v", err)
	}
	if string(payload) != "v1" {
		t.Fatalf("second read returned %q, want v1", payload)
	}
	// Non-vacuity: without this the test passes against a proxy that caches nothing at all, which
	// is every proxy including the one that just forwards.
	if cluster.CacheOpsForTest("get", "hit") == before {
		t.Error("the second read was not served from the cache; the proxy forwarded both and this " +
			"test would pass with the cache removed entirely")
	}
}

// TestAWriteThroughTheProxyInvalidatesExactly is the thesis, reached over the wire protocol.
//
// The write is ordinary SQL from an ordinary client. The proxy is what turns it into an exact
// invalidation — including maintaining the version column the application has never heard of.
func TestAWriteThroughTheProxyInvalidatesExactly(t *testing.T) {
	db, _ := startProxy(t, proxy.RefuseOpaqueWrites)
	const id = 9_400_002
	seedRow(t, id, "v1")

	read := fmt.Sprintf("SELECT payload FROM entities WHERE id = %d", id)
	var payload []byte
	if err := db.QueryRowContext(context.Background(), read).Scan(&payload); err != nil {
		t.Fatalf("warm: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), read).Scan(&payload); err != nil { // now cached
		t.Fatalf("warm again: %v", err)
	}

	if _, err := db.ExecContext(context.Background(), fmt.Sprintf(
		"UPDATE entities SET payload = 'v2' WHERE id = %d", id)); err != nil {
		t.Fatalf("update through the proxy: %v", err)
	}

	if err := db.QueryRowContext(context.Background(), read).Scan(&payload); err != nil {
		t.Fatalf("read after write: %v", err)
	}
	if string(payload) != "v2" {
		t.Fatalf("read after write returned %q, want v2 — the proxy served a stale entry", payload)
	}
}

// TestTheVersionColumnIsMaintainedForTheApplication pins the mechanism the test above depends on.
//
// Without it the invalidation is a tombstone carrying a version no newer than the cached entry,
// which the compare-and-set rejects — leaving a stale entry nothing will ever clear.
func TestTheVersionColumnIsMaintainedForTheApplication(t *testing.T) {
	db, _ := startProxy(t, proxy.RefuseOpaqueWrites)
	const id = 9_400_003
	seedRow(t, id, "v1")

	var before uint64
	if err := db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT version FROM entities WHERE id = %d", id)).
		Scan(&before); err != nil {
		t.Fatalf("read version: %v", err)
	}

	if _, err := db.ExecContext(context.Background(), fmt.Sprintf(
		"UPDATE entities SET payload = 'v2' WHERE id = %d", id)); err != nil {
		t.Fatalf("update: %v", err)
	}

	var after uint64
	if err := db.QueryRowContext(context.Background(), fmt.Sprintf("SELECT version FROM entities WHERE id = %d", id)).
		Scan(&after); err != nil {
		t.Fatalf("read version after: %v", err)
	}
	if after <= before {
		t.Errorf("version went %d → %d: the application's statement said nothing about the version "+
			"column, so the proxy had to maintain it and did not", before, after)
	}
}

// TestAWriteItCannotResolveIsRefused — the honest failure.
//
// Forwarding it would let the write land with the version column unchanged, so every invalidation
// path would lose its compare-and-set and the stale entries would never clear. A clear error at the
// point of the mistake beats silent staleness discovered later by a customer.
func TestAWriteItCannotResolveIsRefused(t *testing.T) {
	db, _ := startProxy(t, proxy.RefuseOpaqueWrites)

	_, err := db.ExecContext(context.Background(), "UPDATE entities SET status = 1 WHERE tenant_id = 424242")
	if err == nil {
		t.Fatal("a write the proxy cannot resolve to specific rows was accepted")
	}
	if !strings.Contains(err.Error(), "cannot resolve") {
		t.Errorf("the error does not explain the problem: %v", err)
	}
}

// And the same write is allowed when the operator has said the application maintains the version
// column itself. The policy exists because that is a real deployment, not because refusing is
// merely inconvenient.
func TestAnOpaqueWriteIsAllowedWhenTheOperatorOptsIn(t *testing.T) {
	db, _ := startProxy(t, proxy.ForwardOpaqueWrites)

	if _, err := db.ExecContext(context.Background(),
		"UPDATE entities SET status = 1, version = version + 1 WHERE tenant_id = 424243"); err != nil {
		t.Fatalf("opaque write was refused despite the opt-in: %v", err)
	}
}

// A read inside a transaction must not come from the cache: the transaction may have already
// written rows nobody else can see, and the cache is nobody else.
func TestAReadInsideATransactionIsNotServedFromTheCache(t *testing.T) {
	db, cluster := startProxy(t, proxy.RefuseOpaqueWrites)
	const id = 9_400_004
	seedRow(t, id, "v1")

	read := fmt.Sprintf("SELECT payload FROM entities WHERE id = %d", id)
	var payload []byte
	for i := 0; i < 2; i++ { // warm it
		if err := db.QueryRowContext(context.Background(), read).Scan(&payload); err != nil {
			t.Fatalf("warm: %v", err)
		}
	}

	// One connection, so BEGIN and the read that follows are the same session.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	before := cluster.CacheOpsForTest("get", "hit")
	if err := conn.QueryRowContext(ctx, read).Scan(&payload); err != nil {
		t.Fatalf("read in transaction: %v", err)
	}
	if cluster.CacheOpsForTest("get", "hit") != before {
		t.Error("a read inside a transaction was served from the cache")
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// A prepared write must not be able to break invalidation.
//
// This is not a hypothetical path: go-sql-driver prepares any statement given arguments, which is
// how most applications write. If a prepared UPDATE reaches the database without its version bump,
// every invalidation for that row loses its compare-and-set and the stale entry never clears —
// silently, which is the failure this project exists to eliminate.
func TestAPreparedWriteCannotBreakInvalidation(t *testing.T) {
	db, _ := startProxy(t, proxy.RefuseOpaqueWrites)
	const id = 9_400_010
	seedRow(t, id, "v1")

	ctx := context.Background()
	read := fmt.Sprintf("SELECT payload FROM entities WHERE id = %d", id)
	var payload []byte
	for i := 0; i < 2; i++ { // warm it
		if err := db.QueryRowContext(ctx, read).Scan(&payload); err != nil {
			t.Fatalf("warm: %v", err)
		}
	}

	// Arguments, so the driver uses COM_STMT_PREPARE / COM_STMT_EXECUTE rather than plain text.
	if _, err := db.ExecContext(ctx,
		"UPDATE entities SET payload = ? WHERE id = ?", "v2", id); err != nil {
		t.Fatalf("prepared update: %v", err)
	}

	if err := db.QueryRowContext(ctx, read).Scan(&payload); err != nil {
		t.Fatalf("read after prepared write: %v", err)
	}
	if string(payload) != "v2" {
		t.Fatalf("read after a PREPARED write returned %q, want v2 — the version bump and the "+
			"invalidation did not happen on the prepared path", payload)
	}
}

// And the refusal has to hold on the prepared path too, or it is trivially bypassed by adding an
// argument to the statement.
func TestAPreparedOpaqueWriteIsAlsoRefused(t *testing.T) {
	db, _ := startProxy(t, proxy.RefuseOpaqueWrites)

	_, err := db.ExecContext(context.Background(),
		"UPDATE entities SET status = ? WHERE tenant_id = ?", 1, 424244)
	if err == nil {
		t.Fatal("an opaque write was accepted because it was prepared; the refusal is bypassable")
	}
	if !strings.Contains(err.Error(), "cannot resolve") {
		t.Errorf("the error does not explain the problem: %v", err)
	}
}

// A prepared point read should be served from the cache, which is the whole point of phase D.
func TestAPreparedReadIsServedFromTheCache(t *testing.T) {
	db, cluster := startProxy(t, proxy.RefuseOpaqueWrites)
	const id = 9_400_011
	seedRow(t, id, "v1")

	ctx := context.Background()
	const q = "SELECT payload FROM entities WHERE id = ?"
	var payload []byte
	if err := db.QueryRowContext(ctx, q, id).Scan(&payload); err != nil {
		t.Fatalf("first prepared read: %v", err)
	}

	before := cluster.CacheOpsForTest("get", "hit")
	if err := db.QueryRowContext(ctx, q, id).Scan(&payload); err != nil {
		t.Fatalf("second prepared read: %v", err)
	}
	if string(payload) != "v1" {
		t.Fatalf("prepared read returned %q, want v1", payload)
	}
	if cluster.CacheOpsForTest("get", "hit") == before {
		t.Error("a prepared point read was not served from the cache")
	}
}
