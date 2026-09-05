//go:build integration

package cache_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
)

// TestMain enforces CONTRIBUTING.md rule 2 mechanically: no goroutine without an owner. A leaked
// connection-pool goroutine here would be a leaked goroutine per request in the engine.
//
// The check runs after the container is torn down, and ignores testcontainers' own reaper, which
// deliberately holds a connection for the lifetime of the test process.
func TestMain(m *testing.M) {
	code := m.Run()
	tearDown()
	if code == 0 {
		if err := goleak.Find(
			goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
			goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		); err != nil {
			fmt.Fprintf(os.Stderr, "goroutine leak: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

var (
	once     sync.Once
	addr     string
	valkey   testcontainers.Container
	startErr error
)

func newClient(ctx context.Context, t *testing.T) *cache.Client {
	t.Helper()

	once.Do(func() { addr, valkey, startErr = startValkey(ctx) })
	if startErr != nil {
		t.Fatalf("start valkey: %v", startErr)
	}
	_ = valkey

	c, err := cache.New(ctx, cache.Options{
		Addresses: []string{addr},
		TTL:       time.Hour,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func startValkey(ctx context.Context) (string, testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "valkey/valkey:8-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		return "", nil, err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return "", nil, err
	}
	port, err := c.MappedPort(ctx, "6379/tcp")
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%s:%s", host, port.Port()), c, nil
}

func tearDown() {
	if valkey == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = valkey.Terminate(ctx)
}

func TestSetThenGetReturnsTheEntry(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	want := cache.Entry{RowVersion: 42, FillVersion: 99, Payload: []byte("payload")}
	if err := c.Set(ctx, "entities:1", want); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, hit, err := c.Get(ctx, "entities:1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Fatal("Get reported a miss for a key that was just set")
	}
	if got.RowVersion != want.RowVersion || got.FillVersion != want.FillVersion ||
		string(got.Payload) != string(want.Payload) {
		t.Errorf("Get returned %+v, want %+v", got, want)
	}
}

func TestMissIsNotAnError(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	// A miss is the normal path, not a failure. Returning an error would make every cold read look
	// like a fault in the metrics and drown the ones that matter.
	_, hit, err := c.Get(ctx, "entities:does-not-exist")
	if err != nil {
		t.Fatalf("Get on a missing key returned an error: %v", err)
	}
	if hit {
		t.Error("Get reported a hit for a key that was never set")
	}
}

func TestEntriesExpire(t *testing.T) {
	ctx := context.Background()
	newClient(ctx, t) // ensures the container is up and gives the test its cleanup

	short, err := cache.New(ctx, cache.Options{Addresses: []string{addr}, TTL: 300 * time.Millisecond})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	defer func() { _ = short.Close() }()

	if err := short.Set(ctx, "entities:ttl", cache.Entry{RowVersion: 1, Payload: []byte("x")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// In Phase 1 the TTL is the ONLY thing bounding staleness, so it has to actually work. From
	// Phase 2 it becomes a backstop behind exact invalidation rather than the strategy.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, hit, err := short.Get(ctx, "entities:ttl")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !hit {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("the entry never expired")
}

func TestCorruptEntryIsReportedNotServed(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	if err := c.SetRawForTest(ctx, "entities:corrupt", []byte("nonsense")); err != nil {
		t.Fatalf("SetRawForTest: %v", err)
	}

	// Corruption must be distinguishable from absence. Silently treating it as a miss would let a
	// systematic encoding bug present itself as a mysterious drop in hit rate.
	_, _, err := c.Get(ctx, "entities:corrupt")
	if !errors.Is(err, cache.ErrCorruptEntry) {
		t.Errorf("Get on a corrupt entry returned %v, want ErrCorruptEntry", err)
	}
}

func TestDeleteRemovesTheEntry(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	if err := c.Set(ctx, "entities:del", cache.Entry{RowVersion: 1, Payload: []byte("x")}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := c.Delete(ctx, "entities:del"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, hit, err := c.Get(ctx, "entities:del"); err != nil || hit {
		t.Errorf("after Delete: hit=%v err=%v, want a clean miss", hit, err)
	}
}

func TestClientRejectsNoAddresses(t *testing.T) {
	ctx := context.Background()

	// Failing at boot beats discovering on the first request that the cache was never configured.
	if _, err := cache.New(ctx, cache.Options{TTL: time.Hour}); err == nil {
		t.Error("cache.New accepted a config with no addresses")
	}
}

func TestClientRejectsANonPositiveTTL(t *testing.T) {
	ctx := context.Background()

	// A zero TTL in Redis means "never expire". In Phase 1 the TTL is the only bound on staleness,
	// so accepting zero would silently turn a bounded cache into an unbounded one.
	if _, err := cache.New(ctx, cache.Options{Addresses: []string{addr}}); err == nil {
		t.Error("cache.New accepted a zero TTL")
	}
}

func TestConcurrentSetsAndGetsAreSafe(t *testing.T) {
	ctx := context.Background()
	c := newClient(ctx, t)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := fmt.Sprintf("entities:conc-%d", g)
			for i := 0; i < 100; i++ {
				if err := c.Set(ctx, key, cache.Entry{RowVersion: uint64(i), Payload: []byte("x")}); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				if _, _, err := c.Get(ctx, key); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
