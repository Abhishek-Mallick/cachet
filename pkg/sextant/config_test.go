package sextant_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/pkg/sextant"
)

// A configuration that loads and then measures nothing is the worst outcome this component has, so
// most of what follows is about refusals — and about the refusals saying which line to change.

const validSextantYAML = `
tier: version
origin:
  dsns: ["u:p@tcp(127.0.0.1:3306)/app"]
  table: orders
  key_column: id
  version_column: row_version
cache:
  addrs: ["127.0.0.1:6379"]
  key_template: "order:{key}"
  codec: json
  version_field: row_version
keys:
  source: binlog
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sextant.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

// TestAConfigLoadsOntoTheDefaults. A file mentioning one setting must not zero every other one —
// including the propagation bound, where a zero means "report every in-flight invalidation".
func TestAConfigLoadsOntoTheDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := sextant.LoadConfig(writeConfig(t, validSextantYAML))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Bound.CDCLagBound == 0 || cfg.Bound.MaxClockSkew == 0 {
		t.Error("the propagation bound was zeroed by a file that never mentioned it")
	}
	if cfg.Sampling.Batch == 0 || cfg.Sampling.Interval == 0 {
		t.Error("the sampling settings were zeroed by a file that never mentioned them")
	}
	if cfg.SLOWindow == 0 {
		t.Error("the SLO window was zeroed")
	}
}

// TestTheShippedExampleIsValid. An example that does not load is a worse introduction than none.
func TestTheShippedExampleIsValid(t *testing.T) {
	t.Parallel()

	cfg, err := sextant.LoadConfig("../../examples/sextant/sextant.yaml")
	if err != nil {
		t.Fatalf("the shipped example does not load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("the shipped example does not validate: %v", err)
	}
}

// Every one of these is a configuration somebody would plausibly write, and every one of them
// would otherwise produce a verifier that runs and measures the wrong thing — or nothing.
func TestARefusedConfigSaysWhichLineToChange(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ body, want string }{
		"a version tier with no version column": {
			strings.Replace(validSextantYAML, "  version_column: row_version\n", "", 1),
			"origin.version_column",
		},
		"a version tier with a codec that cannot extract one": {
			strings.Replace(validSextantYAML, "codec: json", "codec: raw", 1),
			"cannot extract",
		},
		"a value tier with nothing to compare": {
			strings.Replace(validSextantYAML, "tier: version", "tier: value", 1),
			"origin.value_columns",
		},
		"an unknown tier": {
			strings.Replace(validSextantYAML, "tier: version", "tier: strong", 1),
			"unknown tier",
		},
		"an unknown codec": {
			strings.Replace(validSextantYAML, "codec: json", "codec: protobuf", 1),
			"unknown codec",
		},
		"no origin": {
			strings.Replace(validSextantYAML, `  dsns: ["u:p@tcp(127.0.0.1:3306)/app"]`, "  dsns: []", 1),
			"origin.dsns",
		},
		"no cache": {
			strings.Replace(validSextantYAML, `  addrs: ["127.0.0.1:6379"]`, "  addrs: []", 1),
			"cache.addrs",
		},
		"no key template": {
			strings.Replace(validSextantYAML, `  key_template: "order:{key}"`, `  key_template: ""`, 1),
			"cache.key_template",
		},
		"an unknown key source": {
			strings.Replace(validSextantYAML, "source: binlog", "source: guess", 1),
			"keys.source",
		},
		"a file source with no file": {
			strings.Replace(validSextantYAML, "source: binlog", "source: file", 1),
			"keys.file",
		},
		"hash sharding with one writer": {
			strings.Replace(validSextantYAML, "  key_column: id", "  key_column: id\n  shard: hash", 1),
			"use single",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sextant.LoadConfig(writeConfig(t, tc.body))
			if err == nil {
				t.Fatal("a configuration that would measure the wrong thing was accepted")
			}
			if tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not name %q:\n  %v", tc.want, err)
			}
		})
	}
}

// TestAZeroPropagationBoundIsRefused. It is the setting that decides what counts as a violation,
// and zero makes every healthy system report violations continuously.
func TestAZeroPropagationBoundIsRefused(t *testing.T) {
	t.Parallel()

	body := validSextantYAML + `
bound:
  write_path_invalidation_budget: 0s
  cdc_lag_bound: 0s
  max_clock_skew: 0s
`
	_, err := sextant.LoadConfig(writeConfig(t, body))
	if err == nil {
		t.Fatal("a zero propagation bound was accepted")
	}
	if !strings.Contains(err.Error(), "in-flight") {
		t.Errorf("the refusal does not explain the consequence: %v", err)
	}
}
