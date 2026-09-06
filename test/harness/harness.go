// Package harness brings up a Cachet engine against the test environment.
//
// It exists so that an end-to-end test says what it is testing rather than how to assemble a
// cluster. Every suite that uses it gets the same wiring, which is what makes "it passed for me"
// mean something.
package harness

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// DefaultShards matches test/env/compose.yml.
var DefaultShards = []config.Shard{
	{ID: "shard0", DSN: "root:cachet@tcp(127.0.0.1:3316)/cachet?parseTime=true&interpolateParams=true"},
	{ID: "shard1", DSN: "root:cachet@tcp(127.0.0.1:3317)/cachet?parseTime=true&interpolateParams=true"},
	{ID: "shard2", DSN: "root:cachet@tcp(127.0.0.1:3318)/cachet?parseTime=true&interpolateParams=true"},
}

// DefaultCacheAddr matches test/env/compose.yml.
const DefaultCacheAddr = "127.0.0.1:6379"

// Cluster is a running engine plus the shards behind it.
type Cluster struct {
	Addrs  []net.Addr
	Shards map[storage.ShardID]*storage.Shard
	Router *storage.Router
	Cache  *cache.Client

	stop func()
}

// Stop shuts the engine down and waits for it to drain. It is safe to call more than once, so a
// test can stop the cluster explicitly and still rely on cleanup.
func (c *Cluster) Stop() { c.stop() }

// Start brings up an in-process engine listening on the given addresses.
//
// The engine runs in-process rather than as a spawned binary so that a failing test can inspect the
// shards directly — a consistency failure you cannot explain is one you will eventually delete
// (test/README.md).
func Start(ctx context.Context, t *testing.T, listen ...string) *Cluster {
	t.Helper()
	return start(ctx, t, nil, false, listen...)
}

// CacheOptions configures a cached cluster.
type CacheOptions struct {
	TTL time.Duration

	// SynchronousInvalidation mirrors the engine setting of the same name. Turning it off lets a
	// test prove that a guarantee holds on the session watermark ALONE, with no invalidation
	// helping — which is the only way to know which mechanism is actually carrying it.
	SynchronousInvalidation bool
}

// StartCached brings up an engine backed by the test environment's cache, with invalidation on.
func StartCached(ctx context.Context, t *testing.T, ttl time.Duration, listen ...string) *Cluster {
	t.Helper()
	return StartCachedWith(ctx, t, CacheOptions{TTL: ttl, SynchronousInvalidation: true}, listen...)
}

// StartCachedWith brings up a cached engine with explicit options.
//
// Cached and uncached clusters are started by the same code path so that a difference measured
// between them is the cache, and not some other divergence in how the two were assembled.
func StartCachedWith(ctx context.Context, t *testing.T, opts CacheOptions, listen ...string) *Cluster {
	t.Helper()

	EnsureEnvironment(ctx, t)
	c, err := cache.New(ctx, cache.Options{Addresses: []string{DefaultCacheAddr}, TTL: opts.TTL})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// The compose stack's cache is shared and persistent, so entries survive between test runs. A
	// test asserting "the first read is a miss" would then pass on a clean machine and fail on the
	// second run — the classic order-dependent flake. Each cached cluster starts from an empty
	// cache, which is what "every suite brings its own environment" means for cache state.
	//
	// This is safe because tests within a package run sequentially unless they call t.Parallel(),
	// and the e2e tests deliberately do not.
	if err := c.Flush(ctx); err != nil {
		t.Fatalf("flush cache: %v", err)
	}

	cluster := start(ctx, t, c, opts.SynchronousInvalidation, listen...)
	cluster.Cache = c
	return cluster
}

func start(ctx context.Context, t *testing.T, cacheClient engine.Cache, syncInvalidation bool, listen ...string) *Cluster {
	t.Helper()

	EnsureEnvironment(ctx, t)

	shards := make(map[storage.ShardID]*storage.Shard, len(DefaultShards))
	ids := make([]storage.ShardID, 0, len(DefaultShards))
	for _, sc := range DefaultShards {
		id := storage.ShardID(sc.ID)
		sh, err := storage.OpenShard(ctx, id, sc.DSN, storage.NewClock(time.Now))
		if err != nil {
			t.Fatalf("open shard %s: %v", id, err)
		}
		t.Cleanup(func() { _ = sh.Close() })
		shards[id] = sh
		ids = append(ids, id)
	}

	router, err := storage.NewRouter(ids)
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	eng, err := engine.New(engine.Options{
		Router:                  router,
		Shards:                  shards,
		Cache:                   cacheClient,
		MaxSessionShards:        64,
		MaxClockSkew:            250 * time.Millisecond,
		SynchronousInvalidation: syncInvalidation,
		Version:                 "test",
	})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	srv, err := engine.NewServer(ctx, eng, engine.ServerOptions{
		Listen:       listen,
		DrainTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}

	serveCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(serveCtx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Error("engine did not shut down")
			}
		})
	}
	t.Cleanup(stop)

	return &Cluster{Addrs: srv.Addrs(), Shards: shards, Router: router, stop: stop}
}

// Client dials one of the cluster's listeners.
func (c *Cluster) Client(t *testing.T, addr net.Addr) cachetv1.CacheServiceClient {
	t.Helper()

	target := "passthrough:///" + addr.String()
	if addr.Network() == "unix" {
		target = "unix://" + addr.String()
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return cachetv1.NewCacheServiceClient(conn)
}

// SocketPath returns a Unix socket path short enough to bind.
//
// t.TempDir() on macOS routinely exceeds the kernel's 104-byte sun_path limit once the test name is
// appended, so this exists to test the transport rather than the platform's path length.
func SocketPath(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "ch")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "c.sock")
}

// EnsureEnvironment brings the compose stack up if it is not already reachable.
//
// A suite that silently skips when its dependencies are missing is a suite that stops protecting
// anything the first time someone forgets a step, so this starts the stack rather than skipping.
func EnsureEnvironment(ctx context.Context, t *testing.T) {
	t.Helper()

	if reachable(ctx) {
		return
	}

	t.Log("test environment is not reachable; running `make env-up`")
	cmd := exec.CommandContext(ctx, "make", "env-up")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make env-up failed: %v\n%s", err, out)
	}
	if !reachable(ctx) {
		t.Fatal("test environment is still not reachable after `make env-up`")
	}
}

func reachable(ctx context.Context) bool {
	d := net.Dialer{Timeout: 500 * time.Millisecond}
	for _, sc := range DefaultShards {
		addr := hostPort(sc.DSN)
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return false
		}
		_ = conn.Close()
	}
	return true
}

// hostPort extracts "127.0.0.1:3316" from a MySQL DSN.
func hostPort(dsn string) string {
	_, rest, ok := strings.Cut(dsn, "tcp(")
	if !ok {
		return ""
	}
	addr, _, _ := strings.Cut(rest, ")")
	return addr
}

// repoRoot locates the module root from this file's compile-time path, so tests can shell out to
// make regardless of which package directory they run in.
func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine the repository root")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}
