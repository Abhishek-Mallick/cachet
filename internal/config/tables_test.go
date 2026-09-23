package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/config"
)

// A declared table is the one thing in this file that changes what Cachet caches, so the tests are
// about the declaration surviving the trip from YAML intact and about every refusal being one an
// operator can act on.

const validConfig = `
listen: ["tcp://:9090"]
shards:
  - {id: shard0, dsn: "root:x@tcp(127.0.0.1:3306)/cachet"}
  - {id: shard1, dsn: "root:x@tcp(127.0.0.1:3307)/cachet"}
topologies:
  - name: main
    shards: [shard0, shard1]
tables:
  - name: entities
    topology: main
    primary_key: [id]
    version_column: version
    columns:
      - {name: id, type: uint64}
      - {name: tenant_id, type: uint32}
      - {name: status, type: uint8}
      - {name: payload, type: bytes}
      - {name: version, type: uint64}
    predicates:
      - match: [tenant_id, status]
        set: [status]
`

// fixtureFingerprint is what the compiled-in declaration produced. It is written here rather than
// imported so that a change to either side is a visible, deliberate edit.
const fixtureFingerprint = "ab32c55e3e5bbf6a"

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cachet.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// TestADeclaredTableSurvivesTheRoundTrip. The column list is positional — a column's index is part
// of the row encoding and of the fingerprint — so YAML order is not cosmetic.
func TestADeclaredTableSurvivesTheRoundTrip(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(write(t, validConfig), nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Tables) != 1 {
		t.Fatalf("loaded %d tables, want 1", len(cfg.Tables))
	}

	descs, err := cfg.Descriptors()
	if err != nil {
		t.Fatalf("Descriptors: %v", err)
	}
	d := descs[0]
	if d.Name != "entities" {
		t.Errorf("table name = %q, want \"entities\"", d.Name)
	}
	for i, want := range []string{"id", "tenant_id", "status", "payload", "version"} {
		if got := d.Columns[i].Name; got != want {
			t.Errorf("column %d = %q, want %q", i, got, want)
		}
	}
	if len(d.PrimaryKey) != 1 || d.PrimaryKey[0].Name != "id" {
		t.Errorf("primary key = %v, want [id]", d.PrimaryKey)
	}
	if d.VersionColumn.Name != "version" {
		t.Errorf("version column = %q, want \"version\"", d.VersionColumn.Name)
	}

	specs := cfg.Tables[0].PredicateSpecs()
	if len(specs) != 1 || len(specs[0].Match) != 2 || specs[0].Set[0] != "status" {
		t.Errorf("predicates = %v, want one matching on tenant_id and status, setting status", specs)
	}

	shards, err := cfg.ShardsFor("entities")
	if err != nil {
		t.Fatalf("ShardsFor: %v", err)
	}
	if len(shards) != 2 || shards[0].ID != "shard0" || shards[1].ID != "shard1" {
		t.Errorf("shards = %v, want shard0 then shard1", shards)
	}
}

// TestTheDeclaredFixtureFingerprintIsUnchanged is the test that protects every cache entry in
// existence at the moment this config lands.
//
// The fingerprint is derived from the column list, and an entry whose fingerprint does not match
// reads as a MISS. If declaring the fixture table in YAML produced a different fingerprint than
// the compiled-in declaration did, the upgrade would silently cold-start every deployment's cache.
func TestTheDeclaredFixtureFingerprintIsUnchanged(t *testing.T) {
	t.Parallel()

	cfg, err := config.Load(write(t, validConfig), nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	descs, err := cfg.Descriptors()
	if err != nil {
		t.Fatalf("Descriptors: %v", err)
	}
	if got := descs[0].Fingerprint; got != fixtureFingerprint {
		t.Errorf("declaring the fixture table in config produced fingerprint %q, "+
			"but the compiled-in declaration produced %q. Every existing cache entry would read as "+
			"a miss.", got, fixtureFingerprint)
	}
}

// Each of these is a config an operator could plausibly write, and each one would fail later and
// more confusingly than it fails here.
func TestARefusedDeclarationSaysWhatIsWrong(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct{ body, want string }{
		"no tables at all": {
			strings.Replace(validConfig, "tables:", "unused:", 1),
			"cachetctl config migrate",
		},
		"no topologies": {
			strings.Replace(validConfig, "topologies:", "unused:", 1),
			"cachetctl config migrate",
		},
		"a topology naming a shard that is not declared": {
			strings.Replace(validConfig, "shards: [shard0, shard1]", "shards: [shard0, shard9]", 1),
			"which no entry under shards: declares",
		},
		"a topology naming one shard twice": {
			strings.Replace(validConfig, "shards: [shard0, shard1]", "shards: [shard0, shard0]", 1),
			"twice",
		},
		"a table naming a topology that is not declared": {
			strings.Replace(validConfig, "topology: main", "topology: elsewhere", 1),
			"which is not declared",
		},
		"a table naming no topology": {
			strings.Replace(validConfig, "    topology: main\n", "", 1),
			"names no topology",
		},
		"a predicate on a column the table does not have": {
			strings.Replace(validConfig, "match: [tenant_id, status]", "match: [region]", 1),
			"which is not declared",
		},
		"a version column that is not a column": {
			strings.Replace(validConfig, "version_column: version", "version_column: revision", 1),
			"revision",
		},
		"a primary key that is not a column": {
			strings.Replace(validConfig, "primary_key: [id]", "primary_key: [pk]", 1),
			"pk",
		},
		"a column type Cachet does not carry": {
			strings.Replace(validConfig, "{name: payload, type: bytes}", "{name: payload, type: float}", 1),
			"float",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := config.Load(write(t, tc.body), nil)
			if err == nil {
				t.Fatal("accepted a declaration it must refuse")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not mention %q:\n  %v", tc.want, err)
			}
		})
	}
}

// TestTwoDeclarationsOfOneTableAreRefused: two shapes for one set of cache entries, and the
// entries carry a fingerprint of whichever shape happened to write them.
func TestTwoDeclarationsOfOneTableAreRefused(t *testing.T) {
	t.Parallel()

	body := validConfig + `
  - name: entities
    topology: main
    primary_key: [id]
    version_column: version
    columns:
      - {name: id, type: uint64}
      - {name: version, type: uint64}
`
	_, err := config.Load(write(t, body), nil)
	if err == nil || !strings.Contains(err.Error(), "duplicate table") {
		t.Fatalf("err = %v, want a duplicate-table refusal", err)
	}
}

// ─── migration ──────────────────────────────────────────────────────────────────

const preDeclarationConfig = `# A config written before Cachet had declared tables.
listen: ["tcp://:9090"]

shards:
  # Three shards, because the demo needs routing to be real.
  - {id: shard0, dsn: "root:x@tcp(127.0.0.1:3306)/cachet"}
  - {id: shard1, dsn: "root:x@tcp(127.0.0.1:3307)/cachet"}

consistency:
  entry_ttl: 2h   # deliberately long; invalidation is what keeps entries fresh
`

// TestAMigratedConfigLoadsAndKeepsEveryCacheEntry is what the migration is FOR.
//
// A breaking change with no mechanical path is one users pay for twice. The output has to validate
// on the first try, and the fingerprint has to be the one the compiled-in declaration produced — a
// different one would read every existing entry as a miss and cold-start the deployment's cache
// during an upgrade.
func TestAMigratedConfigLoadsAndKeepsEveryCacheEntry(t *testing.T) {
	t.Parallel()

	migrated, err := config.Migrate([]byte(preDeclarationConfig))
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	cfg, err := config.Load(write(t, string(migrated)), nil)
	if err != nil {
		t.Fatalf("the migrated config does not load: %v\n\n%s", err, migrated)
	}

	descs, err := cfg.Descriptors()
	if err != nil {
		t.Fatalf("Descriptors: %v", err)
	}
	if len(descs) != 1 || descs[0].Name != "entities" {
		t.Fatalf("migrated to %d tables, want entities", len(descs))
	}
	if got := descs[0].Fingerprint; got != fixtureFingerprint {
		t.Errorf("the migrated declaration has fingerprint %q, want %q — every existing cache "+
			"entry would read as a miss", got, fixtureFingerprint)
	}

	// The topology must hold every shard, which is what the old build did: one table, on all of
	// them. A migration that dropped one would route part of the key space to a database that
	// never received those rows.
	shards, err := cfg.ShardsFor("entities")
	if err != nil {
		t.Fatalf("ShardsFor: %v", err)
	}
	if len(shards) != 2 {
		t.Errorf("the migrated topology holds %d shards, want 2", len(shards))
	}

	// Conditional writes were possible before the migration and must stay possible after it.
	if specs := cfg.Tables[0].PredicateSpecs(); len(specs) != 1 {
		t.Errorf("the migrated table declares %d predicates, want the one that already worked", len(specs))
	}
}

// TestMigrationKeepsWhatThePersonWrote. A config file is something a human wrote, with comments
// explaining why a timeout is what it is. Re-serialising the document would delete all of it and
// the operator would find out by missing it later.
func TestMigrationKeepsWhatThePersonWrote(t *testing.T) {
	t.Parallel()

	migrated, err := config.Migrate([]byte(preDeclarationConfig))
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	for _, want := range []string{
		"# A config written before Cachet had declared tables.",
		"# Three shards, because the demo needs routing to be real.",
		"entry_ttl: 2h   # deliberately long; invalidation is what keeps entries fresh",
	} {
		if !strings.Contains(string(migrated), want) {
			t.Errorf("the migration lost:\n  %s", want)
		}
	}
}

// TestMigratingTwiceIsRefused: running it again must not append a second declaration, which would
// be a duplicate-table refusal at the next boot and a confusing one, since the operator did the
// thing the previous error told them to do.
func TestMigratingTwiceIsRefused(t *testing.T) {
	t.Parallel()

	once, err := config.Migrate([]byte(preDeclarationConfig))
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := config.Migrate(once); !errors.Is(err, config.ErrAlreadyDeclared) {
		t.Fatalf("err = %v, want ErrAlreadyDeclared", err)
	}
}

func TestMigratingAConfigWithNoShardsIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := config.Migrate([]byte("listen: [\"tcp://:9090\"]\n")); err == nil {
		t.Fatal("built a topology out of no shards")
	}
}
