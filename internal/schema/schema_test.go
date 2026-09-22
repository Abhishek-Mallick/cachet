package schema_test

import (
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

func entities() schema.TableConfig {
	return schema.TableConfig{
		Name:          "entities",
		PrimaryKey:    []string{"id"},
		VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: "uint64"},
			{Name: "tenant_id", Type: "uint32"},
			{Name: "status", Type: "uint8"},
			{Name: "payload", Type: "bytes"},
			{Name: "version", Type: "uint64"},
		},
	}
}

func TestTheFixtureTableStillDescribes(t *testing.T) {
	t.Parallel()

	d, err := schema.NewDescriptor(entities())
	if err != nil {
		t.Fatalf("the table Cachet shipped with no longer describes: %v", err)
	}
	if d.Name != "entities" || len(d.Columns) != 5 {
		t.Errorf("descriptor = %s with %d columns", d.Name, len(d.Columns))
	}
	if got := d.Column("payload"); got == nil || got.Index != 3 {
		t.Errorf("payload resolved to %+v, want index 3", got)
	}
}

// Identifiers reach SQL, and they come only from configuration — but "only from configuration" is
// a claim about deployment, not a control. The control is this.
func TestAnIdentifierThatCouldReachSQLIsRefused(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{
		"", " ", "entities; DROP TABLE users", "entities`", "`entities`", "entities'",
		"1entities", "entities-1", "entities.users", "entities users", "ent\x00ities",
		"e" + strings.Repeat("x", 64), "entities\n", "--", "/*", "entities/*x*/",
	} {
		cfg := entities()
		cfg.Name = bad
		if _, err := schema.NewDescriptor(cfg); err == nil {
			t.Errorf("accepted table name %q", bad)
		}
	}
	for _, bad := range []string{"", "payload; --", "pay`load", "pay load"} {
		cfg := entities()
		cfg.Columns[3].Name = bad
		if _, err := schema.NewDescriptor(cfg); err == nil {
			t.Errorf("accepted column name %q", bad)
		}
	}
}

// Hazard 1, and the one that would never surface by accident.
//
// Under a case- or accent-insensitive collation MySQL treats 'Ann' and 'ann' as the SAME row, while
// Go produces two distinct cache keys. The result is an invalidation that silently misses: the row
// is updated under one key and cached under the other, and nothing reports it.
func TestACaseInsensitiveStringPrimaryKeyIsRefused(t *testing.T) {
	t.Parallel()

	for _, collation := range []string{
		"utf8mb4_0900_ai_ci", "utf8mb4_general_ci", "latin1_swedish_ci", "utf8mb4_0900_as_ci",
	} {
		cfg := schema.TableConfig{
			Name: "users", PrimaryKey: []string{"email"}, VersionColumn: "version",
			Columns: []schema.ColumnConfig{
				{Name: "email", Type: "string", Collation: collation},
				{Name: "version", Type: "uint64"},
			},
		}
		_, err := schema.NewDescriptor(cfg)
		if err == nil {
			t.Errorf("accepted a %s primary key: two cache keys can name one row", collation)
			continue
		}
		if !strings.Contains(err.Error(), "collation") {
			t.Errorf("the error does not name the problem: %v", err)
		}
	}

	// A binary or case-sensitive collation is fine, and so is a non-PK column of any collation.
	for _, collation := range []string{"utf8mb4_0900_bin", "utf8mb4_bin", "binary", "utf8mb4_0900_as_cs"} {
		cfg := schema.TableConfig{
			Name: "users", PrimaryKey: []string{"email"}, VersionColumn: "version",
			Columns: []schema.ColumnConfig{
				{Name: "email", Type: "string", Collation: collation},
				{Name: "version", Type: "uint64"},
			},
		}
		if _, err := schema.NewDescriptor(cfg); err != nil {
			t.Errorf("refused a %s primary key: %v", collation, err)
		}
	}
}

func TestTheVersionColumnMustExistAndBeAnInteger(t *testing.T) {
	t.Parallel()

	cfg := entities()
	cfg.VersionColumn = "nope"
	if _, err := schema.NewDescriptor(cfg); err == nil {
		t.Error("accepted a version column that is not in the table")
	}

	cfg = entities()
	cfg.Columns[4].Type = "string"
	if _, err := schema.NewDescriptor(cfg); err == nil {
		t.Error("accepted a non-integer version column; every compare-and-set orders on it")
	}

	cfg = entities()
	cfg.Columns[4].Nullable = true
	if _, err := schema.NewDescriptor(cfg); err == nil {
		t.Error("accepted a nullable version column: a NULL version cannot be compared")
	}
}

func TestThePrimaryKeyMustBeDeclaredAndNotNullable(t *testing.T) {
	t.Parallel()

	cfg := entities()
	cfg.PrimaryKey = nil
	if _, err := schema.NewDescriptor(cfg); err == nil {
		t.Error("accepted a table with no primary key")
	}

	cfg = entities()
	cfg.PrimaryKey = []string{"missing"}
	if _, err := schema.NewDescriptor(cfg); err == nil {
		t.Error("accepted a primary key column that is not in the table")
	}

	cfg = entities()
	cfg.Columns[0].Nullable = true
	if _, err := schema.NewDescriptor(cfg); err == nil {
		t.Error("accepted a nullable primary key: it cannot name a row")
	}
}

// The fingerprint is what makes a schema change safe without a flush: every old entry mismatches
// and reads as a miss. So it must change when the shape changes, and NOT when something cosmetic
// does.
func TestTheFingerprintTracksShapeAndNothingElse(t *testing.T) {
	t.Parallel()

	base, err := schema.NewDescriptor(entities())
	if err != nil {
		t.Fatal(err)
	}

	same, _ := schema.NewDescriptor(entities())
	if base.Fingerprint != same.Fingerprint {
		t.Error("the same declaration produced two fingerprints; every deploy would miss every entry")
	}

	for name, mutate := range map[string]func(*schema.TableConfig){
		"a column added": func(c *schema.TableConfig) {
			c.Columns = append(c.Columns, schema.ColumnConfig{Name: "extra", Type: "uint32"})
		},
		"a column renamed":  func(c *schema.TableConfig) { c.Columns[1].Name = "renamed" },
		"a column retyped":  func(c *schema.TableConfig) { c.Columns[1].Type = "uint64" },
		"column order":      func(c *schema.TableConfig) { c.Columns[1], c.Columns[2] = c.Columns[2], c.Columns[1] },
		"nullability":       func(c *schema.TableConfig) { c.Columns[1].Nullable = true },
		"the table renamed": func(c *schema.TableConfig) { c.Name = "other" },
		"the primary key":   func(c *schema.TableConfig) { c.PrimaryKey = []string{"id", "tenant_id"} },
	} {
		cfg := entities()
		mutate(&cfg)
		d, err := schema.NewDescriptor(cfg)
		if err != nil {
			continue // refused for another reason, which is also fine
		}
		if d.Fingerprint == base.Fingerprint {
			t.Errorf("%s did not change the fingerprint: old entries would decode into the new shape", name)
		}
	}
}

// Quoting is the last line between a config value and a SQL statement.
func TestIdentifiersAreQuotedForSQL(t *testing.T) {
	t.Parallel()

	d, err := schema.NewDescriptor(entities())
	if err != nil {
		t.Fatal(err)
	}
	if got := d.QuotedName(); got != "`entities`" {
		t.Errorf("QuotedName = %s", got)
	}
	if got := d.Column("payload").Quoted(); got != "`payload`" {
		t.Errorf("column Quoted = %s", got)
	}
}
