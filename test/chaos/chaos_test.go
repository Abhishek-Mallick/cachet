//go:build chaos

// Package chaos injects faults and asserts that Cachet is CAUGHT OUT by none of them — and, more
// importantly, that each one is EXPLAINED.
//
// Surviving a fault is table stakes; every cache survives its backing store being slow. The claim
// this suite exists to defend is narrower and harder: that when something goes wrong, an operator
// can find out what, from a metric or a command, without reading the source. An entry in FAULTS.md
// that can only say "the system recovered" has not met the bar.
//
// Two rules hold throughout:
//
//  1. Every injection is proven to have fired. A toxic that silently failed to apply produces a
//     serene green test that asserts nothing, which is the same vacuity trap the conformance suite
//     defends against by running its cells against a deliberately broken cache.
//  2. Correctness is asserted; timing is not. These tests run on whatever host CI gives them, so a
//     claim like "p99 stays under 20ms" would be measuring the host. Latency claims belong in the
//     benchmark suite, on hardware that is disclosed.
package chaos_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// Shards and cache as reached THROUGH Toxiproxy. The engine believes these are the databases.
var proxiedShards = []config.Shard{
	{ID: "shard0", DSN: "root:cachet@tcp(127.0.0.1:23306)/cachet?parseTime=true&interpolateParams=true"},
	{ID: "shard1", DSN: "root:cachet@tcp(127.0.0.1:23307)/cachet?parseTime=true&interpolateParams=true"},
	{ID: "shard2", DSN: "root:cachet@tcp(127.0.0.1:23308)/cachet?parseTime=true&interpolateParams=true"},
}

const proxiedCacheAddr = "127.0.0.1:26379"

// faultRecord is one row of FAULTS.md. It is written by the test that injects the fault, so the
// document cannot drift from the suite: a fault nobody tests produces no row.
type faultRecord struct {
	Number      int    `json:"number"`
	Title       string `json:"title"`
	Injection   string `json:"injection"`
	Claim       string `json:"claim"`
	Fired       string `json:"fired"`       // the evidence the injection actually took effect
	Observed    string `json:"observed"`    // what Cachet did
	Explanation string `json:"explanation"` // the command or metric that says WHY
}

var (
	recordsMu sync.Mutex
	records   []faultRecord
)

// record files one fault's result. Called on the success path only: a failing test must not write
// a row claiming the fault was handled.
func record(t *testing.T, r faultRecord) {
	t.Helper()

	if r.Fired == "" {
		t.Fatalf("fault %d recorded without evidence that the injection fired", r.Number)
	}
	recordsMu.Lock()
	defer recordsMu.Unlock()
	records = append(records, r)
}

func TestMain(m *testing.M) {
	code := run(m)
	os.Exit(code)
}

func run(m *testing.M) int {
	if err := ensureProxies(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: toxiproxy not reachable at %s: %v\n", toxiproxyAPI, err)
		fmt.Fprintf(os.Stderr, "chaos: bring it up with: make env-chaos-up\n")
		return 1
	}

	code := m.Run()

	// Written even on failure: a partial run still records which faults were proven, and the
	// renderer is what turns them into FAULTS.md. Nothing here is typed by hand.
	if err := writeRecords(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: writing fault records: %v\n", err)
		return 1
	}
	return code
}

// ensureProxies creates the four proxies if they are absent. Idempotent, because the container
// outlives any one test run and a second run must not fail on "proxy already exists".
func ensureProxies() error {
	t := &testing.T{} // toxiproxyDo only uses t for Helper(); nothing here fails the harness

	var existing map[string]proxy
	ctx := context.Background()
	if err := toxiproxyDo(ctx, t, "GET", "/proxies", nil, &existing); err != nil {
		return err
	}
	for _, p := range proxies {
		if _, ok := existing[p.Name]; ok {
			continue
		}
		if err := toxiproxyDo(ctx, t, "POST", "/proxies", p, nil); err != nil {
			return fmt.Errorf("create proxy %s: %w", p.Name, err)
		}
	}
	return nil
}

func writeRecords() error {
	recordsMu.Lock()
	defer recordsMu.Unlock()

	if len(records) == 0 {
		return nil
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Number < records[j].Number })

	out := filepath.Join("testdata", "faults.json")
	if err := os.MkdirAll(filepath.Dir(out), 0o750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(out, append(b, '\n'), 0o600)
}

// startProxied brings up an engine whose every dependency is reachable only through Toxiproxy.
func startProxied(ctx context.Context, t *testing.T, opts harness.CacheOptions) *harness.Cluster {
	t.Helper()

	resetToxiproxy(ctx, t)
	// Cleanup must run even when ctx is done, or a crashed test leaves toxics installed and the
	// next test fails for a reason that has nothing to do with it.
	//nolint:contextcheck // deliberate, as above.
	t.Cleanup(func() { resetToxiproxy(context.Background(), t) })

	opts.Shards = proxiedShards
	opts.CacheAddr = proxiedCacheAddr
	if opts.TTL == 0 {
		opts.TTL = time.Hour
	}
	return harness.StartCachedWith(ctx, t, opts, "tcp://127.0.0.1:0")
}
