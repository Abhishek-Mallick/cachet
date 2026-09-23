package engine_test

import (
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/test/fixtures/table"
)

// What an engine will agree to serve.
//
// Cachet no longer has a built-in table, so the declaration arrives from config and the engine has
// to say what it can and cannot carry. Every refusal here is a boot failure an operator can read;
// the alternative is an engine that starts and then writes rows using another table's columns.

func newEngine(t *testing.T, tables ...*schema.Descriptor) (*engine.Engine, error) {
	t.Helper()

	router, err := storage.NewRouter([]storage.ShardID{"shard0"})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return engine.New(engine.Options{
		Router: router,
		Shards: map[storage.ShardID]*storage.Shard{"shard0": {}},
		Tables: tables,
	})
}

func descriptorOf(t *testing.T, cfg schema.TableConfig) *schema.Descriptor {
	t.Helper()
	d, err := schema.NewDescriptor(cfg)
	if err != nil {
		t.Fatalf("NewDescriptor: %v", err)
	}
	return d
}

// fixtureShape returns the shape this build's storage path carries, under a chosen name.
func fixtureShape(name string) schema.TableConfig {
	cfg := table.Entities().TableConfig
	cfg.Name = name
	return cfg
}

func TestAnEngineWithNoDeclaredTableRefusesToStart(t *testing.T) {
	t.Parallel()

	_, err := newEngine(t)
	if err == nil {
		t.Fatal("started with no table declared")
	}
	if !strings.Contains(err.Error(), "no built-in table") {
		t.Errorf("error does not say why there is nothing to serve: %v", err)
	}
}

// TestTheDeclaredTableMayHaveAnyName is the increment this release actually delivers.
//
// The storage path still carries one column shape, but the NAME was never part of that shape. A
// deployment whose table is called `widgets` and whose columns match is served correctly, which is
// more than "configure the name of the table we always meant" — it is the first declaration Cachet
// reads rather than assumes.
func TestTheDeclaredTableMayHaveAnyName(t *testing.T) {
	t.Parallel()

	d := descriptorOf(t, fixtureShape("widgets"))
	e, err := newEngine(t, d)
	if err != nil {
		t.Fatalf("refused a correctly-shaped table because of its name: %v", err)
	}

	got, err := e.Table("widgets")
	if err != nil {
		t.Fatalf("Table: %v", err)
	}
	if got.Name != "widgets" {
		t.Errorf("served table = %q, want \"widgets\"", got.Name)
	}
	if _, err := e.Table("entities"); err == nil {
		t.Error("an engine serving `widgets` also claimed to serve `entities`")
	}

	// The fingerprint is derived from the declaration, table name included, so `widgets` has its
	// own. What must hold is that it is the DECLARATION's — not this build's fixture one, which
	// would make every entry read as a miss against a cache client configured from the config.
	if got.Fingerprint == table.Descriptor().Fingerprint {
		t.Error("a differently-named table produced the fixture table's fingerprint")
	}
	if got.Fingerprint != descriptorOf(t, fixtureShape("widgets")).Fingerprint {
		t.Error("the served table's fingerprint is not the declaration's")
	}
}

// TestAKeyForAnotherTableIsRefused: routing and cache identity both come from the key, so a key
// naming a table this engine does not serve would be cached under an identity no invalidation path
// can reproduce.
func TestAKeyForAnotherTableIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := engine.ParseKey("widgets", "widgets:42"); err != nil {
		t.Fatalf("ParseKey refused its own table: %v", err)
	}
	if _, err := engine.ParseKey("widgets", "entities:42"); err == nil {
		t.Error("ParseKey accepted a key naming a table this engine does not serve")
	}
}

// Each of these is a declaration an operator could reasonably write, and each one would otherwise
// be discovered as rows written with the wrong columns.
func TestAShapeTheStoragePathCannotCarryIsRefused(t *testing.T) {
	t.Parallel()

	fewerColumns := fixtureShape("widgets")
	fewerColumns.Columns = append(fewerColumns.Columns[:2:2], fewerColumns.Columns[3:]...)

	renamedColumn := fixtureShape("widgets")
	renamedColumn.Columns[1].Name = "account_id"

	retypedColumn := fixtureShape("widgets")
	retypedColumn.Columns[1].Type = schema.Uint64

	reordered := fixtureShape("widgets")
	reordered.Columns[1], reordered.Columns[2] = reordered.Columns[2], reordered.Columns[1]

	nullable := fixtureShape("widgets")
	nullable.Columns[3].Nullable = true

	otherKey := fixtureShape("widgets")
	otherKey.PrimaryKey = []string{"tenant_id"}

	for name, tc := range map[string]struct {
		cfg  schema.TableConfig
		want string
	}{
		"a shorter column list":      {fewerColumns, "columns"},
		"a renamed column":           {renamedColumn, "account_id"},
		"a retyped column":           {retypedColumn, "tenant_id"},
		"a reordered column list":    {reordered, "column 1"},
		"a column made nullable":     {nullable, "nullable"},
		"another primary key column": {otherKey, "primary key"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := newEngine(t, descriptorOf(t, tc.cfg))
			if err == nil {
				t.Fatal("started with a table its storage path cannot carry")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not mention %q:\n  %v", tc.want, err)
			}
		})
	}
}

// TestASecondTableIsRefusedRatherThanHalfServed. The cache, the wire protocol and the proxy all
// carry rows generically; the engine's own read and write paths do not yet. Accepting a second
// declaration would mean routing to a storage call that still names the first table's columns.
func TestASecondTableIsRefusedRatherThanHalfServed(t *testing.T) {
	t.Parallel()

	_, err := newEngine(t,
		descriptorOf(t, fixtureShape("widgets")),
		descriptorOf(t, fixtureShape("gadgets")),
	)
	if err == nil {
		t.Fatal("accepted two tables")
	}
	for _, want := range []string{"widgets", "gadgets", "serves one"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n  %v", want, err)
		}
	}
}
