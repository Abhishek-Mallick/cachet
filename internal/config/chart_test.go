package config_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/config"
)

// The Helm chart renders a config, and nothing used to load it.
//
// It had never worked. The ConfigMap emitted `host`, `port` and `database` per shard where the
// loader needs a `dsn`, and the Deployment mounted CACHET_DB_USERNAME and CACHET_DB_PASSWORD from a
// Secret into variables the loader does not read. CI ran `helm lint` and `helm template` at release,
// and both are satisfied by a chart that RENDERS — neither ever asked whether Cachet would accept
// what came out.
//
// Two tests close that gap from two directions, and neither one re-implements Helm:
//
//   - the CHART side: the template emits a dsn, and the variables it names are the ones the
//     Deployment mounts. Both are the defect, stated as assertions over the template text.
//   - the LOADER side: a config in the shape the chart emits loads, credentials and all.
//
// The end-to-end check — `helm template` piped into the loader — runs in CI, where Helm is already
// installed. A worse version of this test parsed the template with regular expressions, dropped the
// line it was meant to be checking, and reported that the chart emitted no DSN. Re-implementing a
// template engine to test a template is a way to be confidently wrong.

const (
	chartConfigMapPath = "../../deploy/helm/cachet/templates/configmap.yaml"
	chartEnginePath    = "../../deploy/helm/cachet/templates/engine.yaml"
)

func readChartFile(t *testing.T, path string) string {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(raw)
}

// TestTheChartEmitsAShardShapeTheLoaderReads is the first half of the defect, asserted.
//
// `config.Shard` has exactly two fields. A chart emitting anything else produces a ConfigMap that
// renders perfectly and boots nothing.
func TestTheChartEmitsAShardShapeTheLoaderReads(t *testing.T) {
	t.Parallel()

	body := readChartFile(t, chartConfigMapPath)
	shardBlock := blockAfter(t, body, "shards:")

	if !strings.Contains(shardBlock, "dsn:") {
		t.Error("the chart's shard block emits no dsn; config.Shard has an id and a dsn, and " +
			"anything else renders a ConfigMap that boots nothing")
	}
	// The fields the broken version emitted. They are legitimate as VALUES inputs — the chart builds
	// a DSN out of them — so what is forbidden is emitting them as config KEYS.
	for _, key := range []string{"host:", "port:", "database:"} {
		if strings.Contains(shardBlock, key) {
			t.Errorf("the chart's shard block emits %q, which config.Shard does not have", key)
		}
	}
}

// TestTheChartAndTheDeploymentAgreeOnTheSecretVariables is the second half.
//
// The ConfigMap names variables; the Deployment mounts them from a Secret. Nothing checks that the
// two lists are the same list, and when they were not, the credentials simply never arrived.
func TestTheChartAndTheDeploymentAgreeOnTheSecretVariables(t *testing.T) {
	t.Parallel()

	configMap := readChartFile(t, chartConfigMapPath)
	deployment := readChartFile(t, chartEnginePath)

	referenced := envRefsIn(configMap)
	if len(referenced) == 0 {
		t.Fatal("the ConfigMap references no ${VAR}; either credentials are inlined into it — " +
			"readable by anything that can list ConfigMaps — or they never reach the engine")
	}

	for _, name := range referenced {
		// `- name: CACHET_DB_PASSWORD` in the container's env block.
		if !strings.Contains(deployment, "name: "+name) {
			t.Errorf("the ConfigMap expands %s, and the Deployment does not mount it; "+
				"the engine would refuse to start naming that variable", name)
		}
	}
}

var envRefRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// envRefsIn collects the `${VAR}` references a template EMITS.
//
// Comment lines are skipped. The chart documents this mechanism in a comment that names `${VAR}`
// literally, and counting that as a reference would have this test demand the Deployment mount a
// variable called VAR — which is how a test ends up asserting something nobody meant.
func envRefsIn(body string) []string {
	seen := map[string]bool{}
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		for _, m := range envRefRe.FindAllStringSubmatch(line, -1) {
			if !seen[m[1]] {
				seen[m[1]] = true
				out = append(out, m[1])
			}
		}
	}
	return out
}

// TestAConfigInTheShapeTheChartEmitsLoads is the loader side.
//
// Hand-written rather than rendered, and kept in the shape the assertions above pin. What it proves
// is the thing that was never true: a ConfigMap of this shape, plus a Secret mounted as those
// variables, boots.
func TestAConfigInTheShapeTheChartEmitsLoads(t *testing.T) {
	t.Parallel()

	body := `
listen: ["tcp://:9090"]
shards:
  - id: shard0
    dsn: "${CACHET_DB_USERNAME}:${CACHET_DB_PASSWORD}@tcp(mysql-0.mysql:3306)/app"
  - id: shard1
    dsn: "${CACHET_DB_USERNAME}:${CACHET_DB_PASSWORD}@tcp(mysql-1.mysql:3306)/app"
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
cache:
  addresses:
    - 10.0.0.1:6379
consistency:
  default_level: SESSION
  max_clock_skew: 250ms
`
	cfg, err := config.Load(write(t, body), map[string]string{
		"CACHET_DB_USERNAME": "cachet",
		"CACHET_DB_PASSWORD": "s3cret",
	})
	if err != nil {
		t.Fatalf("a config in the shape the chart emits does not load: %v", err)
	}
	if len(cfg.Shards) != 2 {
		t.Fatalf("loaded %d shards, want 2", len(cfg.Shards))
	}
	if cfg.Shards[0].DSN != "cachet:s3cret@tcp(mysql-0.mysql:3306)/app" {
		t.Errorf("DSN = %q; the credentials from the Secret did not reach it", cfg.Shards[0].DSN)
	}
	if len(cfg.Tables) != 1 {
		t.Errorf("the config declares %d tables; Cachet has no built-in one", len(cfg.Tables))
	}
}

// blockAfter returns the indented block following a key, which is how a YAML template's sections
// are delimited when it is being read as text rather than parsed.
func blockAfter(t *testing.T, body, key string) string {
	t.Helper()

	i := strings.Index(body, key)
	if i < 0 {
		t.Fatalf("the chart no longer has a %q section; this test is checking the wrong thing", key)
	}
	rest := body[i+len(key):]

	// The block ends at the next line whose indentation returns to the key's own, which for the
	// ConfigMap's embedded config.yaml is four spaces.
	var out []string
	for _, line := range strings.Split(rest, "\n")[1:] {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		if indent <= 4 && !strings.HasPrefix(trimmed, "{{") && !strings.HasPrefix(trimmed, "-") {
			break
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// TestAMissingSecretKeyIsABootFailureNamingIt. Expanding an unset variable to an empty string
// produces an authentication error against the database, reported by MySQL, blamed on the
// credentials, with nothing in it pointing at the config.
func TestAMissingSecretKeyIsABootFailureNamingIt(t *testing.T) {
	t.Parallel()

	body := `
listen: ["tcp://:9090"]
shards:
  - {id: shard0, dsn: "${CACHET_DB_USERNAME}:${CACHET_DB_PASSWORD}@tcp(db:3306)/app"}
` + fixtureTableYAML

	_, err := config.Load(write(t, body), map[string]string{"CACHET_DB_USERNAME": "cachet"})
	if err == nil {
		t.Fatal("a DSN with an unset variable was accepted")
	}
	if !strings.Contains(err.Error(), "CACHET_DB_PASSWORD") {
		t.Errorf("the error does not name the variable that is missing: %v", err)
	}
}

// TestAnEmptyPasswordIsHonoured: a database with no password is a real thing, and refusing one
// would force an operator to choose between a working deployment and an honest Secret.
func TestAnEmptyPasswordIsHonoured(t *testing.T) {
	t.Parallel()

	body := `
listen: ["tcp://:9090"]
shards:
  - {id: shard0, dsn: "${CACHET_DB_USERNAME}:${CACHET_DB_PASSWORD}@tcp(db:3306)/app"}
` + fixtureTableYAML

	cfg, err := config.Load(write(t, body), map[string]string{
		"CACHET_DB_USERNAME": "root", "CACHET_DB_PASSWORD": "",
	})
	if err != nil {
		t.Fatalf("a set-but-empty password was refused: %v", err)
	}
	if cfg.Shards[0].DSN != "root:@tcp(db:3306)/app" {
		t.Errorf("DSN = %q", cfg.Shards[0].DSN)
	}
}

// TestADollarInAPasswordIsNotExpanded. `$` is a legal password character, and expanding a bare
// `$NAME` would corrupt a DSN that was already correct.
func TestADollarInAPasswordIsNotExpanded(t *testing.T) {
	t.Parallel()

	body := `
listen: ["tcp://:9090"]
shards:
  - {id: shard0, dsn: "root:p$$w0rd$HOME@tcp(db:3306)/app"}
` + fixtureTableYAML

	cfg, err := config.Load(write(t, body), map[string]string{"HOME": "/root"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Shards[0].DSN != "root:p$$w0rd$HOME@tcp(db:3306)/app" {
		t.Errorf("DSN = %q; a literal $ in a password was expanded", cfg.Shards[0].DSN)
	}
}

// TestAMalformedReferenceIsRefused. Left alone it would be sent to MySQL as part of a username.
func TestAMalformedReferenceIsRefused(t *testing.T) {
	t.Parallel()

	body := `
listen: ["tcp://:9090"]
shards:
  - {id: shard0, dsn: "root:${UNCLOSED@tcp(db:3306)/app"}
` + fixtureTableYAML

	if _, err := config.Load(write(t, body), map[string]string{}); err == nil {
		t.Error("a DSN with an unclosed ${ reference was accepted")
	}
}
