package ctl_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/cdc"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/ctl"
)

func testConfig() config.Config {
	cfg := config.Default()
	cfg.Shards = []config.Shard{
		{ID: "shard0", DSN: "root:x@tcp(127.0.0.1:3306)/cachet"},
		{ID: "shard1", DSN: "root:x@tcp(127.0.0.1:3307)/cachet"},
		{ID: "shard2", DSN: "root:x@tcp(127.0.0.1:3308)/cachet"},
	}
	cfg.Cache.Addresses = []string{"10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379"}
	return cfg
}

func TestRingReportsBothRings(t *testing.T) {
	t.Parallel()

	// An operator debugging a hot spot needs to see both rings at once. Showing only one invites
	// exactly the assumption the independent-ring design exists to break: that a cache node and a
	// shard are the same thing wearing two names.
	r, err := ctl.Ring(testConfig(), 3000)
	if err != nil {
		t.Fatalf("Ring: %v", err)
	}

	if len(r.CacheNodes) != 3 {
		t.Errorf("CacheNodes = %v, want 3 nodes", r.CacheNodes)
	}
	if len(r.Shards) != 3 {
		t.Errorf("Shards = %v, want 3 shards", r.Shards)
	}
}

func TestRingReportsKeyShareOfEachCacheNode(t *testing.T) {
	t.Parallel()

	r, err := ctl.Ring(testConfig(), 30000)
	if err != nil {
		t.Fatalf("Ring: %v", err)
	}

	total := 0.0
	for _, n := range r.CacheNodes {
		share, ok := r.CacheShare[n]
		if !ok {
			t.Fatalf("no key share reported for cache node %s", n)
		}
		if share <= 0 {
			t.Errorf("cache node %s reports a %v share", n, share)
		}
		total += share
	}
	if total < 0.99 || total > 1.01 {
		t.Errorf("cache node shares sum to %v, want 1", total)
	}
}

func TestRingWithNoCacheIsNotAnError(t *testing.T) {
	t.Parallel()

	// Running uncached is a supported configuration — it is the baseline every benchmark row is
	// compared against. `ring status` has to describe it rather than fail on it.
	cfg := testConfig()
	cfg.Cache.Addresses = nil

	r, err := ctl.Ring(cfg, 1000)
	if err != nil {
		t.Fatalf("Ring on an uncached config: %v", err)
	}
	if len(r.CacheNodes) != 0 {
		t.Errorf("CacheNodes = %v on an uncached config, want none", r.CacheNodes)
	}
	if len(r.Shards) != 3 {
		t.Errorf("Shards = %v, want the shards to still be reported", r.Shards)
	}
}

func TestRingRendersReadably(t *testing.T) {
	t.Parallel()

	r, err := ctl.Ring(testConfig(), 3000)
	if err != nil {
		t.Fatalf("Ring: %v", err)
	}

	out := r.String()
	for _, want := range []string{"10.0.0.1:6379", "shard0", "cache ring", "shard ring"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered output does not mention %q:\n%s", want, out)
		}
	}
}

func TestLocateReportsCacheNodeAndShard(t *testing.T) {
	t.Parallel()

	// "Where does this key live?" is two answers, not one, and they come from two independent
	// rings. Reporting only the shard is what makes people believe the cache is sharded with it.
	loc, err := ctl.Locate(testConfig(), "entities:42")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}

	if loc.Key != "entities:42" {
		t.Errorf("Key = %q, want entities:42", loc.Key)
	}
	if loc.CacheNode == "" {
		t.Error("Locate reported no cache node")
	}
	if loc.Shard == "" {
		t.Error("Locate reported no shard")
	}
}

func TestLocateIsStableForTheSameKey(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	first, err := ctl.Locate(cfg, "entities:42")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	second, err := ctl.Locate(cfg, "entities:42")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}

	if first != second {
		t.Errorf("Locate returned %+v then %+v for the same key", first, second)
	}
}

func TestLocateRejectsAMalformedKey(t *testing.T) {
	t.Parallel()

	// A key the tool half-understands is worse than one it refuses: it would route somewhere
	// deterministic and send the operator to inspect a node that never held the row.
	for _, bad := range []string{"", "entities", "entities:", "orders:1", "entities:abc"} {
		if _, err := ctl.Locate(testConfig(), bad); err == nil {
			t.Errorf("Locate accepted the malformed key %q", bad)
		}
	}
}

func TestLocateWithNoCacheReportsTheShardOnly(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.Cache.Addresses = nil

	loc, err := ctl.Locate(cfg, "entities:42")
	if err != nil {
		t.Fatalf("Locate on an uncached config: %v", err)
	}
	if loc.CacheNode != "" {
		t.Errorf("CacheNode = %q on an uncached config, want empty", loc.CacheNode)
	}
	if loc.Shard == "" {
		t.Error("Locate reported no shard")
	}
}

func writeCheckpoint(t *testing.T, dir, shard string, pos cdc.Position) {
	t.Helper()

	cp := cdc.NewFileCheckpoint(filepath.Join(dir, shard+".pos"))
	if err := cp.Save(pos); err != nil {
		t.Fatalf("save checkpoint for %s: %v", shard, err)
	}
}

func TestCheckpointsReportEveryShard(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckpoint(t, dir, "shard0", cdc.Position{File: "binlog.000004", Offset: 1234})
	writeCheckpoint(t, dir, "shard1", cdc.Position{File: "binlog.000002", Offset: 99})

	r, err := ctl.Checkpoints(testConfig(), dir)
	if err != nil {
		t.Fatalf("Checkpoints: %v", err)
	}
	if len(r.Shards) != 3 {
		t.Fatalf("reported %d shards, want 3", len(r.Shards))
	}

	byID := make(map[string]ctl.ShardCheckpoint, len(r.Shards))
	for _, s := range r.Shards {
		byID[s.Shard] = s
	}

	if got := byID["shard0"]; !got.Present || got.Position.File != "binlog.000004" || got.Position.Offset != 1234 {
		t.Errorf("shard0 checkpoint = %+v, want binlog.000004:1234", got)
	}
	if got := byID["shard1"]; !got.Present || got.Position.Offset != 99 {
		t.Errorf("shard1 checkpoint = %+v, want offset 99", got)
	}
}

func TestAMissingCheckpointIsReportedNotHidden(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckpoint(t, dir, "shard0", cdc.Position{File: "binlog.000001", Offset: 4})

	// A shard with no checkpoint means its tailer has never committed a position — it is either
	// brand new or it has never run. Omitting the row would present a partial picture as a complete
	// one, and "the tailer for shard2 was never started" is exactly the incident this command exists
	// to make visible.
	r, err := ctl.Checkpoints(testConfig(), dir)
	if err != nil {
		t.Fatalf("Checkpoints: %v", err)
	}

	found := false
	for _, s := range r.Shards {
		if s.Shard == "shard2" {
			found = true
			if s.Present {
				t.Errorf("shard2 reports a checkpoint it does not have: %+v", s)
			}
		}
	}
	if !found {
		t.Error("shard2 is missing from the report entirely")
	}
}

func TestCheckpointsOnAMissingDirectoryIsAnError(t *testing.T) {
	t.Parallel()

	// A typo'd --state-dir must not read as "no tailer has ever run". That mistake would send an
	// operator hunting a CDC outage that is not happening.
	if _, err := ctl.Checkpoints(testConfig(), filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("Checkpoints accepted a state directory that does not exist")
	}
}

func TestCheckpointsRendersReadably(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckpoint(t, dir, "shard0", cdc.Position{File: "binlog.000004", Offset: 1234})

	r, err := ctl.Checkpoints(testConfig(), dir)
	if err != nil {
		t.Fatalf("Checkpoints: %v", err)
	}

	out := r.String()
	if !strings.Contains(out, "binlog.000004:1234") {
		t.Errorf("rendered output does not show the position:\n%s", out)
	}
	if !strings.Contains(out, "no checkpoint") {
		t.Errorf("rendered output does not flag the shards without a checkpoint:\n%s", out)
	}
}

func TestReportsMarshalToJSON(t *testing.T) {
	t.Parallel()

	// Every command speaks JSON as well as prose. An operator reads the prose; a runbook, an alert
	// and a CI check all need to parse it, and a tool that only prints a table forces each of them
	// to grep.
	dir := t.TempDir()
	writeCheckpoint(t, dir, "shard0", cdc.Position{File: "binlog.000004", Offset: 1234})

	ring, err := ctl.Ring(testConfig(), 1000)
	if err != nil {
		t.Fatalf("Ring: %v", err)
	}
	loc, err := ctl.Locate(testConfig(), "entities:42")
	if err != nil {
		t.Fatalf("Locate: %v", err)
	}
	cps, err := ctl.Checkpoints(testConfig(), dir)
	if err != nil {
		t.Fatalf("Checkpoints: %v", err)
	}

	for name, v := range map[string]any{"ring": ring, "locate": loc, "checkpoints": cps} {
		b, err := json.Marshal(v)
		if err != nil {
			t.Errorf("marshal %s: %v", name, err)
			continue
		}
		if !json.Valid(b) || len(b) < 3 {
			t.Errorf("%s marshalled to %q", name, b)
		}
	}
}

func TestCheckpointsIgnoresUnrelatedFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeCheckpoint(t, dir, "shard0", cdc.Position{File: "binlog.000001", Offset: 4})
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatalf("write stray file: %v", err)
	}

	// The report is driven by the configured shards, not by whatever happens to be in the
	// directory. A stray file must not become a phantom shard.
	r, err := ctl.Checkpoints(testConfig(), dir)
	if err != nil {
		t.Fatalf("Checkpoints: %v", err)
	}
	if len(r.Shards) != 3 {
		t.Errorf("reported %d shards, want the 3 that are configured", len(r.Shards))
	}
}
