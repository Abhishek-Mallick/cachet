//go:build integration

package ctl_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/ctl"
)

func TestMain(m *testing.M) {
	code := m.Run()
	teardown()
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
	once      sync.Once
	addr      string
	container testcontainers.Container
	startErr  error
)

func liveConfig(t *testing.T) (config.Config, *cache.Client) {
	t.Helper()

	once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		addr, container, startErr = startValkey(ctx)
	})
	if startErr != nil {
		// Fatal, never skipped: a gate that goes green while testing nothing is worse than no gate.
		t.Fatalf("start valkey: %v", startErr)
	}

	cfg := testConfig()
	cfg.Cache.Addresses = []string{addr}

	c, err := cache.New(context.Background(), cache.Options{Addresses: cfg.Cache.Addresses, TTL: time.Hour})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return cfg, c
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

func teardown() {
	if container == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = container.Terminate(ctx)
}

func TestInspectReportsAMissAsAMiss(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	got, err := ctl.Inspect(ctx, c, cfg, "entities:404")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if got.Cached {
		t.Errorf("Inspect reported a key that was never filled as cached: %+v", got)
	}
	// Routing is still reported. "Not cached" is only actionable alongside "and it would live here".
	if got.CacheNode == "" || got.Shard == "" {
		t.Errorf("Inspect on a miss dropped the routing information: %+v", got)
	}
}

func TestInspectReportsBothVersions(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	if _, err := c.Fill(ctx, "entities:7", cache.Entry{RowVersion: 111, FillVersion: 222, Payload: []byte("x")}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	got, err := ctl.Inspect(ctx, c, cfg, "entities:7")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if !got.Cached {
		t.Fatal("Inspect reported a miss for a key that was just filled")
	}

	// Both versions, never one. They answer different questions — RowVersion orders fills against
	// each other, FillVersion answers freshness — and an operator shown only one of them cannot tell
	// a stale entry from an old row that was read a millisecond ago (CONSISTENCY.md §1).
	if got.Entry.RowVersion != 111 {
		t.Errorf("RowVersion = %d, want 111", got.Entry.RowVersion)
	}
	if got.Entry.FillVersion != 222 {
		t.Errorf("FillVersion = %d, want 222", got.Entry.FillVersion)
	}
}

func TestInspectReportsRemainingTTL(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	if _, err := c.Fill(ctx, "entities:8", cache.Entry{RowVersion: 1, FillVersion: 1}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	got, err := ctl.Inspect(ctx, c, cfg, "entities:8")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	// "When does this expire?" is the second question anyone asks about a cached entry, right after
	// "is it there?".
	if got.TTL <= 0 || got.TTL > time.Hour {
		t.Errorf("TTL = %s, want something inside the configured hour", got.TTL)
	}
}

func TestInspectFlagsANegativeEntry(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	if _, err := c.Fill(ctx, "entities:9", cache.Entry{RowVersion: 5, FillVersion: 5, Negative: true}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	got, err := ctl.Inspect(ctx, c, cfg, "entities:9")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	// A negative entry and an absent one look identical to a reader and are completely different to
	// an operator: one means "we know this row does not exist", the other means "we have not looked".
	if !got.Cached {
		t.Fatal("a negative entry was reported as not cached; it IS cached, as a known absence")
	}
	if !got.Entry.Negative {
		t.Error("Inspect did not flag the entry as negative")
	}
}

func TestInspectRejectsAMalformedKey(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	if _, err := ctl.Inspect(ctx, c, cfg, "orders:1"); err == nil {
		t.Error("Inspect accepted a key for a table the engine does not serve")
	}
}

func TestInvalidateRemovesTheEntryFromReaders(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	if _, err := c.Fill(ctx, "entities:11", cache.Entry{RowVersion: 3, FillVersion: 3, Payload: []byte("old")}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	res, err := ctl.Invalidate(ctx, c, cfg, "entities:11", false)
	if err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if !res.Applied {
		t.Errorf("Invalidate reported the tombstone was not applied: %+v", res)
	}

	if _, hit, err := c.Get(ctx, "entities:11"); err != nil {
		t.Fatalf("Get: %v", err)
	} else if hit {
		t.Error("the entry is still readable after being invalidated")
	}
}

func TestInvalidateStampsAVersionThatBeatsOlderFills(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	if _, err := c.Fill(ctx, "entities:12", cache.Entry{RowVersion: 3, FillVersion: 3}); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	res, err := ctl.Invalidate(ctx, c, cfg, "entities:12", false)
	if err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	// The manual escape hatch has to win against anything already in flight. A tombstone stamped
	// with a low version would lose its compare-and-set to a read that started before the operator
	// ran the command, and the entry the operator just "removed" would quietly come back.
	applied, err := c.Fill(ctx, "entities:12", cache.Entry{RowVersion: res.Version - 1, FillVersion: res.Version - 1})
	if err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if applied {
		t.Error("a fill older than the tombstone was applied; the manual invalidation can be undone by an in-flight read")
	}
}

func TestInvalidateDryRunChangesNothing(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	if _, err := c.Fill(ctx, "entities:13", cache.Entry{RowVersion: 3, FillVersion: 3, Payload: []byte("keep")}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	res, err := ctl.Invalidate(ctx, c, cfg, "entities:13", true)
	if err != nil {
		t.Fatalf("Invalidate dry run: %v", err)
	}
	if !res.DryRun {
		t.Error("the result does not record that it was a dry run")
	}

	// A blast-radius preview that actually fires is worse than no preview at all.
	if _, hit, err := c.Get(ctx, "entities:13"); err != nil {
		t.Fatalf("Get: %v", err)
	} else if !hit {
		t.Error("the dry run invalidated the entry")
	}
}

func TestInvalidateReportsWhereItActed(t *testing.T) {
	ctx := context.Background()
	cfg, c := liveConfig(t)

	res, err := ctl.Invalidate(ctx, c, cfg, "entities:14", false)
	if err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if res.CacheNode == "" || res.Shard == "" {
		t.Errorf("Invalidate did not report where it acted: %+v", res)
	}
	if res.Key != "entities:14" {
		t.Errorf("Key = %q, want entities:14", res.Key)
	}
}

func TestHealthReportsEveryCacheNode(t *testing.T) {
	ctx := context.Background()
	cfg, _ := liveConfig(t)

	h, err := ctl.Health(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if len(h.CacheNodes) != 1 {
		t.Fatalf("reported %d cache nodes, want 1", len(h.CacheNodes))
	}
	if !h.CacheNodes[0].Healthy {
		t.Errorf("a live cache node was reported unhealthy: %+v", h.CacheNodes[0])
	}
	if h.CacheNodes[0].Latency <= 0 {
		t.Errorf("no round-trip latency was measured: %+v", h.CacheNodes[0])
	}
}

func TestHealthReportsAnUnreachableNodeWithoutFailing(t *testing.T) {
	ctx := context.Background()
	cfg, _ := liveConfig(t)

	// 127.0.0.1:1 is reserved and refuses connections.
	cfg.Cache.Addresses = append(cfg.Cache.Addresses, "127.0.0.1:1")

	// A health command that errors out on the first dead node cannot report on the rest of the
	// ring — which is the moment an operator most needs the whole picture.
	h, err := ctl.Health(ctx, cfg, 2*time.Second)
	if err != nil {
		t.Fatalf("Health returned an error instead of reporting the dead node: %v", err)
	}
	if len(h.CacheNodes) != 2 {
		t.Fatalf("reported %d cache nodes, want 2", len(h.CacheNodes))
	}

	var healthy, unhealthy int
	for _, n := range h.CacheNodes {
		if n.Healthy {
			healthy++
		} else {
			unhealthy++
			if n.Error == "" {
				t.Errorf("node %s is unhealthy but reports no reason", n.Address)
			}
		}
	}
	if healthy != 1 || unhealthy != 1 {
		t.Errorf("got %d healthy and %d unhealthy, want 1 of each", healthy, unhealthy)
	}
}
