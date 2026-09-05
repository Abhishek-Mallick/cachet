package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

func TestDefaultsAreValid(t *testing.T) {
	t.Parallel()

	// The defaults have to stand on their own: `cachet` with only shard DSNs supplied must boot.
	// A default set that cannot pass its own validator is a trap for the first user.
	cfg := config.Default()
	cfg.Shards = []config.Shard{{ID: "shard0", DSN: "root:x@tcp(127.0.0.1:3306)/cachet"}}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Default().Validate(): %v", err)
	}
}

func TestNoShardsIsRejected(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() accepted a config with no shards")
	}
	if !strings.Contains(err.Error(), "shards") {
		t.Errorf("error %q does not name the offending field", err)
	}
}

func TestDuplicateShardIDsAreRejected(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Shards = []config.Shard{
		{ID: "shard0", DSN: "a"},
		{ID: "shard0", DSN: "b"},
	}

	// Two shards with one id would silently collapse in the router, and half the key space would
	// route to a database that never received its rows.
	if err := cfg.Validate(); err == nil {
		t.Error("Validate() accepted duplicate shard ids")
	}
}

func TestShardWithoutDSNIsRejected(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Shards = []config.Shard{{ID: "shard0"}}

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() accepted a shard with no DSN")
	}
}

func TestListenAddressesAreParsed(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in      string
		network string
		address string
	}{
		{"tcp://:9090", "tcp", ":9090"},
		{"tcp://127.0.0.1:9090", "tcp", "127.0.0.1:9090"},
		{"unix:///var/run/cachet.sock", "unix", "/var/run/cachet.sock"},
	} {
		got, err := config.ParseListen(tc.in)
		if err != nil {
			t.Errorf("ParseListen(%q): %v", tc.in, err)
			continue
		}
		if got.Network != tc.network || got.Address != tc.address {
			t.Errorf("ParseListen(%q) = %s/%s, want %s/%s", tc.in, got.Network, got.Address, tc.network, tc.address)
		}
	}
}

func TestUnknownListenSchemeIsRejected(t *testing.T) {
	t.Parallel()

	// The sidecar topology is the default and rides on a Unix socket (ADR 0004), so getting this
	// wrong silently would push every deployment onto the slower path without anyone noticing.
	for _, in := range []string{"http://:8080", ":9090", "tcp:/missing-slash", ""} {
		if _, err := config.ParseListen(in); err == nil {
			t.Errorf("ParseListen(%q) succeeded; want an error", in)
		}
	}
}

func TestNoListenAddressIsRejected(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Shards = []config.Shard{{ID: "shard0", DSN: "x"}}
	cfg.Listen = nil

	if err := cfg.Validate(); err == nil {
		t.Error("Validate() accepted a config with nowhere to listen")
	}
}

func TestConsistencySettingsMustBePositive(t *testing.T) {
	t.Parallel()

	// Every field here changes what Cachet PROMISES, not how fast it is (CONSISTENCY.md §9). A
	// nonsensical value must stop the process rather than quietly widen a guarantee.
	for _, tc := range []struct {
		name  string
		apply func(*config.Config)
	}{
		{"negative max clock skew", func(c *config.Config) { c.Consistency.MaxClockSkew = -time.Second }},
		{"zero cdc lag bound", func(c *config.Config) { c.Consistency.CDCLagBound = 0 }},
		{"zero write path budget", func(c *config.Config) { c.Consistency.WritePathInvalidationBudget = 0 }},
		{"zero max affected keys", func(c *config.Config) { c.Consistency.MaxAffectedKeys = 0 }},
		{"zero max session shards", func(c *config.Config) { c.Consistency.MaxSessionShards = 0 }},
		{"zero entry ttl", func(c *config.Config) { c.Consistency.EntryTTL = 0 }},
	} {
		cfg := config.Default()
		cfg.Shards = []config.Shard{{ID: "shard0", DSN: "x"}}
		tc.apply(&cfg)

		if err := cfg.Validate(); err == nil {
			t.Errorf("Validate() accepted %s", tc.name)
		}
	}
}

func TestDefaultLevelIsParsedAndValidated(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Shards = []config.Shard{{ID: "shard0", DSN: "x"}}
	cfg.DefaultLevel = "stronk"

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() accepted an unknown default consistency level")
	}

	cfg.DefaultLevel = "eventual"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(): %v", err)
	}
	if lv := cfg.Level(); lv != consistency.Eventual {
		t.Errorf("Level() = %v, want Eventual", lv)
	}
}

func TestLoadReadsAYAMLFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cachet.yaml")
	writeFile(t, path, `
listen:
  - unix:///tmp/cachet.sock
shards:
  - id: shard0
    dsn: root:cachet@tcp(127.0.0.1:3316)/cachet
  - id: shard1
    dsn: root:cachet@tcp(127.0.0.1:3317)/cachet
consistency:
  max_clock_skew: 500ms
  entry_ttl: 2h
`)

	cfg, err := config.Load(path, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Shards) != 2 {
		t.Errorf("loaded %d shards, want 2", len(cfg.Shards))
	}
	if cfg.Consistency.MaxClockSkew != 500*time.Millisecond {
		t.Errorf("MaxClockSkew = %s, want 500ms", cfg.Consistency.MaxClockSkew)
	}
	if cfg.Consistency.EntryTTL != 2*time.Hour {
		t.Errorf("EntryTTL = %s, want 2h", cfg.Consistency.EntryTTL)
	}
	// Fields the file did not mention must keep their defaults rather than becoming zero values.
	if cfg.Consistency.CDCLagBound != config.Default().Consistency.CDCLagBound {
		t.Errorf("CDCLagBound = %s; an unspecified field must keep its default", cfg.Consistency.CDCLagBound)
	}
}

func TestEnvironmentOverridesTheFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cachet.yaml")
	writeFile(t, path, `
shards:
  - id: shard0
    dsn: from-file
consistency:
  max_clock_skew: 100ms
`)

	cfg, err := config.Load(path, map[string]string{
		"CACHET_CONSISTENCY__MAX_CLOCK_SKEW": "750ms",
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Consistency.MaxClockSkew != 750*time.Millisecond {
		t.Errorf("MaxClockSkew = %s, want the environment's 750ms", cfg.Consistency.MaxClockSkew)
	}
}

func TestLoadRejectsAnInvalidConfig(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cachet.yaml")
	writeFile(t, path, "listen: []\n")

	// Failing fast at boot is the whole point: discovering a bad config on the first user request
	// converts an operator problem into a customer problem (CONTRIBUTING.md rule 15).
	if _, err := config.Load(path, nil); err == nil {
		t.Error("Load accepted a config with no shards and no listeners")
	}
}

func TestLoadReportsAMissingFile(t *testing.T) {
	t.Parallel()

	if _, err := config.Load(filepath.Join(t.TempDir(), "absent.yaml"), nil); err == nil {
		t.Error("Load succeeded on a missing file")
	}
}

func TestGuaranteeSettingsAreEnumerableForLogging(t *testing.T) {
	t.Parallel()

	// CONSISTENCY.md §9 requires these to be logged at boot and exported as gauges. Enumerating
	// them in one place is what stops a new guarantee-affecting setting from being added without
	// anyone being able to see its value in production.
	cfg := config.Default()
	got := cfg.Consistency.GuaranteeSettings()

	for _, want := range []string{
		"max_clock_skew", "cdc_lag_bound", "write_path_invalidation_budget",
		"max_affected_keys", "max_session_shards", "entry_ttl",
	} {
		if _, ok := got[want]; !ok {
			t.Errorf("GuaranteeSettings() is missing %q", want)
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestBootLogRendersDurationsReadably(t *testing.T) {
	t.Parallel()

	// The boot line exists so someone reading an incident timeline can see which guarantees were in
	// force. "14400000000000" does not serve that purpose.
	attrs := config.Default().Consistency.LogAttrs()

	var found bool
	for _, a := range attrs {
		if a.Key != "entry_ttl" {
			continue
		}
		found = true
		if got := a.Value.String(); got != "4h0m0s" {
			t.Errorf("entry_ttl logged as %q, want \"4h0m0s\"", got)
		}
	}
	if !found {
		t.Error("entry_ttl is missing from the boot log attributes")
	}

	// Sorted keys keep the line diffable across restarts.
	for i := 1; i < len(attrs); i++ {
		if attrs[i-1].Key > attrs[i].Key {
			t.Errorf("boot log attributes are not sorted: %q before %q", attrs[i-1].Key, attrs[i].Key)
			break
		}
	}
}
