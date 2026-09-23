package engine

import (
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// What cachet.v1 can and cannot describe.
//
// v1 spells one table's columns into the protocol: Record has tenant_id, status and payload as
// FIELDS. A deployment declaring anything else has no v1 representation, and the shim refuses it
// rather than inventing columns — which would be a wrong answer dressed as a working one.
//
// White-box, because the check runs before any request and there is no way to reach it from
// outside without a live database.

func shapeOf(t *testing.T, mutate func(*schema.TableConfig)) *schema.Descriptor {
	t.Helper()

	cfg := schema.TableConfig{
		Name:          "widgets",
		PrimaryKey:    []string{"id"},
		VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: schema.Uint64},
			{Name: "tenant_id", Type: schema.Uint32},
			{Name: "status", Type: schema.Uint8},
			{Name: "payload", Type: schema.Bytes},
			{Name: "version", Type: schema.Uint64},
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	d, err := schema.NewDescriptor(cfg)
	if err != nil {
		t.Fatalf("NewDescriptor: %v", err)
	}
	return d
}

// TestV1DescribesAnyTableOfItsShape. The NAME was never part of what v1 can express — a deployment
// whose table is called `widgets` and whose columns match is served over v1 correctly.
func TestV1DescribesAnyTableOfItsShape(t *testing.T) {
	t.Parallel()

	if err := checkLegacyShape(shapeOf(t, nil)); err != nil {
		t.Errorf("refused a correctly-shaped table because of its name: %v", err)
	}
}

// Each of these is a declaration an operator could reasonably write, and each one would otherwise
// be discovered as a row whose columns do not mean what the client thinks they mean.
func TestV1RefusesAShapeItCannotDescribe(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		mutate func(*schema.TableConfig)
		want   string
	}{
		"a shorter column list": {
			func(c *schema.TableConfig) {
				c.Columns = append(c.Columns[:2:2], c.Columns[3:]...)
			},
			"five-column row",
		},
		"a renamed column": {
			func(c *schema.TableConfig) { c.Columns[1].Name = "account_id" },
			"account_id",
		},
		"a retyped column": {
			func(c *schema.TableConfig) { c.Columns[1].Type = schema.Uint64 },
			"tenant_id",
		},
		"a reordered column list": {
			func(c *schema.TableConfig) { c.Columns[1], c.Columns[2] = c.Columns[2], c.Columns[1] },
			"column 1",
		},
		"a column made nullable": {
			func(c *schema.TableConfig) { c.Columns[3].Nullable = true },
			"nullable",
		},
		"another primary key column": {
			func(c *schema.TableConfig) { c.PrimaryKey = []string{"tenant_id"} },
			"single `id` column",
		},
		"a version column called something else": {
			func(c *schema.TableConfig) {
				c.Columns[4].Name = "row_version"
				c.VersionColumn = "row_version"
			},
			"row_version",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := checkLegacyShape(shapeOf(t, tc.mutate))
			if err == nil {
				t.Fatal("cachet.v1 claimed to describe a shape it has no fields for")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not mention %q:\n  %v", tc.want, err)
			}
			// Every one of these refusals has to point somewhere, or an operator is left with a
			// protocol that says no and nothing to do about it.
			if !strings.Contains(err.Error(), "cachet.v2") {
				t.Errorf("the refusal does not name the protocol that CAN describe this:\n  %v", err)
			}
		})
	}
}

// TestV1CannotNameATableSoItRefusesToGuess. Record has no table field, so an engine serving several
// tables has nothing to disambiguate a v1 request with — and picking one would serve a different
// table's rows under the same call.
func TestV1CannotNameATableSoItRefusesToGuess(t *testing.T) {
	t.Parallel()

	e := &Engine{
		tables:     map[string]*table{"widgets": {}, "gadgets": {}},
		tableOrder: []string{"widgets", "gadgets"},
	}
	_, err := e.soleTable()
	if err == nil {
		t.Fatal("an engine serving two tables let cachet.v1 pick one")
	}
	for _, want := range []string{"widgets", "gadgets", "cachet.v2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q:\n  %v", want, err)
		}
	}
}
