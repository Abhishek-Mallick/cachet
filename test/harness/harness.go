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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/internal/admission"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/obs"
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

	stop          func()
	originReads   func() (int, error)
	leaseOutcomes func(string) int
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
	return start(ctx, t, nil, CacheOptions{}, listen...)
}

// CacheOptions configures a cached cluster.
type CacheOptions struct {
	TTL time.Duration

	// SynchronousInvalidation mirrors the engine setting of the same name. Turning it off lets a
	// test prove that a guarantee holds on the session watermark ALONE, with no invalidation
	// helping — which is the only way to know which mechanism is actually carrying it.
	SynchronousInvalidation bool

	// Admission decides which keys are worth caching. Nil caches everything, which is what most
	// suites want: they are testing consistency, not cost.
	Admission *admission.Controller

	// LeaseTTL bounds how long one caller may hold the right to fill a key. Zero takes the client
	// default.
	LeaseTTL time.Duration

	// Leases is the wait policy for cache-fill admission. The zero value never waits, which is what
	// most suites want: it keeps them deterministic and fast, since nothing in them is trying to
	// provoke a stampede.
	Leases engine.WaitPolicy

	// MaxAffectedKeys is the conditional-write budget past which exact key resolution is abandoned.
	// Zero takes the default; a conformance test that wants to observe degradation sets it low
	// rather than writing a thousand rows to provoke it.
	MaxAffectedKeys int

	// PreserveCache keeps whatever the cache already holds instead of flushing it at startup.
	//
	// Needed by failover tests, which start a SECOND engine against state the first one left
	// behind. Flushing there would erase the very thing under test and let the new engine pass by
	// reading everything from the database — a green test proving nothing about the cache.
	//
	// Off by default: every other suite wants a clean cache, because the compose stack's cache is
	// shared and persistent, and a test asserting "the first read is a miss" would otherwise pass on
	// a clean machine and fail on the second run.
	PreserveCache bool
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
	c, err := cache.New(ctx, cache.Options{
		Addresses: []string{DefaultCacheAddr},
		TTL:       opts.TTL,
		LeaseTTL:  opts.LeaseTTL,
	})
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
	if !opts.PreserveCache {
		if err := c.Flush(ctx); err != nil {
			t.Fatalf("flush cache: %v", err)
		}
	}

	cluster := start(ctx, t, c, opts, listen...)
	cluster.Cache = c
	return cluster
}

func start(ctx context.Context, t *testing.T, cacheClient engine.Cache, opts CacheOptions, listen ...string) *Cluster {
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

	// A private registry per cluster: suites start several engines in one process, and a shared
	// registry would make one cluster'''s origin reads visible in another'''s assertions.
	metrics, err := obs.NewMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}

	eng, err := engine.New(engine.Options{
		Metrics:                 metrics,
		Router:                  router,
		Shards:                  shards,
		Cache:                   cacheClient,
		MaxSessionShards:        64,
		MaxAffectedKeys:         opts.MaxAffectedKeys,
		Leases:                  opts.Leases,
		Admission:               opts.Admission,
		MaxClockSkew:            250 * time.Millisecond,
		SynchronousInvalidation: opts.SynchronousInvalidation,
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

	return &Cluster{
		Addrs:  srv.Addrs(),
		Shards: shards,
		Router: router,
		stop:   stop,
		originReads: func() (int, error) {
			return int(testutil.ToFloat64(metrics.Origin())), nil
		},
		leaseOutcomes: func(outcome string) int {
			return int(testutil.ToFloat64(metrics.Leases().WithLabelValues(outcome)))
		},
	}
}

// LeaseOutcomesForTest reports how many times each lease outcome has occurred.
//
// It is what lets a stampede test prove it actually observed a stampede: if no caller ever waited,
// the readers were serialised and any low origin count says nothing about the lease.
func (c *Cluster) LeaseOutcomesForTest(outcome string) int {
	return c.leaseOutcomes(outcome)
}

// OriginReadsForTest reports how many reads have reached the database.
//
// It reads the engine's own counter rather than a test-side tally, so what is asserted is the
// number the product reports — the same one a benchmark publishes and an operator alerts on. A
// separate count could agree with the tests and disagree with production.
func (c *Cluster) OriginReadsForTest() (int, error) {
	return c.originReads()
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
