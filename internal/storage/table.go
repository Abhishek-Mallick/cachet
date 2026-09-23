package storage

import (
	"fmt"
	"strings"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// Table is every SQL statement Cachet issues against one user table, built once at boot.
//
// Built once, from identifiers the schema package already validated, because nothing on the request
// path should be concatenating SQL. The only string building that happens per request is the
// placeholder run for a batch, whose length is the only variable part.
type Table struct {
	d *schema.Descriptor

	get      string
	batchGet string // without the placeholder run, which depends on the batch size
	insert   string
	update   string
	del      string
	lockRow  string

	predicates []PredicateSpec
}

// PredicateSpec is a conditional-write shape the deployment permits.
//
// Declared rather than inferred from a request, and validated against the table's indexes at boot.
// See NewTable for why the index requirement is not a performance concern.
type PredicateSpec struct {
	Match []string `koanf:"match"`
	Set   []string `koanf:"set"`
}

// Index is one index as the database reports it, in key order.
type Index struct {
	Name    string
	Columns []string
}

// LiveColumn is one column as INFORMATION_SCHEMA reports it.
type LiveColumn struct {
	Name     string
	Nullable bool
}

// NewTable builds the statements and validates the declared predicates.
//
// A predicate must be covered by a LEADING prefix of some index. This is not tuning: the
// conditional write resolves its affected rows with `SELECT … FOR UPDATE` inside the transaction
// that is about to do the write, and without an index MySQL takes gap locks across the table while
// holding it. That is an outage rather than a slow query, and it only appears under the concurrency
// production has — so it is refused at boot instead of discovered at peak.
func NewTable(d *schema.Descriptor, indexes []Index, predicates ...PredicateSpec) (*Table, error) {
	if d == nil {
		return nil, fmt.Errorf("storage: no descriptor")
	}

	cols := make([]string, len(d.Columns))
	for i := range d.Columns {
		cols[i] = d.Columns[i].Quoted()
	}
	selectList := strings.Join(cols, ", ")

	pk := make([]string, len(d.PrimaryKey))
	for i, c := range d.PrimaryKey {
		pk[i] = c.Quoted() + " = ?"
	}
	pkMatch := strings.Join(pk, " AND ")

	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(d.Columns)), ", ")
	assignments := make([]string, len(d.Columns))
	for i := range d.Columns {
		assignments[i] = d.Columns[i].Quoted() + " = ?"
	}

	t := &Table{
		d:        d,
		get:      "SELECT " + selectList + " FROM " + d.QuotedName() + " WHERE " + pkMatch,
		batchGet: "SELECT " + selectList + " FROM " + d.QuotedName() + " WHERE " + d.PrimaryKey[0].Quoted() + " IN ",
		insert:   "INSERT INTO " + d.QuotedName() + " (" + selectList + ") VALUES (" + placeholders + ")",
		update:   "UPDATE " + d.QuotedName() + " SET " + strings.Join(assignments, ", ") + " WHERE " + pkMatch,
		del:      "DELETE FROM " + d.QuotedName() + " WHERE " + pkMatch,
		lockRow:  "SELECT " + d.VersionColumn.Quoted() + " FROM " + d.QuotedName() + " WHERE " + pkMatch + " FOR UPDATE",
	}

	for _, p := range predicates {
		if err := t.validatePredicate(p, indexes); err != nil {
			return nil, err
		}
		t.predicates = append(t.predicates, p)
	}
	return t, nil
}

// Descriptor is the shape this table's rows have.
func (t *Table) Descriptor() *schema.Descriptor { return t.d }

// GetStmt reads one row by primary key.
func (t *Table) GetStmt() string { return t.get }

// InsertStmt inserts a whole row.
func (t *Table) InsertStmt() string { return t.insert }

// UpdateStmt replaces a whole row by primary key.
func (t *Table) UpdateStmt() string { return t.update }

// DeleteStmt removes a row by primary key.
func (t *Table) DeleteStmt() string { return t.del }

// LockRowStmt reads a row's version FOR UPDATE.
func (t *Table) LockRowStmt() string { return t.lockRow }

// BatchGetStmt reads n rows by primary key.
//
// The placeholder run is the only SQL built per request, and n is the only thing that varies.
// Composite primary keys are not batched here: `WHERE (a,b) IN ((?,?),…)` is a row constructor that
// MyRocks plans differently, so it is a separate step rather than an untested branch.
func (t *Table) BatchGetStmt(n int) (string, error) {
	if len(t.d.PrimaryKey) != 1 {
		return "", fmt.Errorf("storage: %s has a composite primary key; batch reads are single-column only", t.d.Name)
	}
	if n <= 0 {
		return "", fmt.Errorf("storage: batch of %d rows", n)
	}
	return t.batchGet + "(" + strings.TrimSuffix(strings.Repeat("?, ", n), ", ") + ")", nil
}

func (t *Table) validatePredicate(p PredicateSpec, indexes []Index) error {
	if len(p.Match) == 0 {
		return fmt.Errorf("storage: %s: a conditional write must match on at least one column", t.d.Name)
	}
	for _, name := range p.Match {
		if t.d.Column(name) == nil {
			return fmt.Errorf("storage: %s: predicate matches on %q, which is not declared", t.d.Name, name)
		}
	}
	for _, name := range p.Set {
		col := t.d.Column(name)
		if col == nil {
			return fmt.Errorf("storage: %s: predicate sets %q, which is not declared", t.d.Name, name)
		}
		if col == t.d.VersionColumn {
			return fmt.Errorf("storage: %s: predicate sets %q, the version column. Cachet maintains it, "+
				"and a write that set it would order itself against the engine's own versions", t.d.Name, name)
		}
		for _, k := range t.d.PrimaryKey {
			if col == k {
				return fmt.Errorf("storage: %s: predicate sets %q, part of the primary key. "+
					"Moving a row's identity would leave its cache entry naming a row that no longer exists", t.d.Name, name)
			}
		}
	}
	if !coveredByLeadingPrefix(p.Match, indexes) {
		return fmt.Errorf("storage: %s: no index begins with %v, so resolving this predicate would "+
			"gap-lock the table inside the write's own transaction. Add an index, or do not declare the predicate",
			t.d.Name, p.Match)
	}
	return nil
}

// coveredByLeadingPrefix reports whether some index starts with exactly these columns, in any order.
//
// A leading prefix, not merely "contains": MySQL can use `idx(a,b)` for a predicate on `a`, and for
// one on `a AND b`, but not for one on `b` alone — and it is the `b` alone case that silently
// becomes a table scan under a lock.
func coveredByLeadingPrefix(match []string, indexes []Index) bool {
	want := make(map[string]bool, len(match))
	for _, m := range match {
		want[m] = true
	}
	for _, idx := range indexes {
		if len(idx.Columns) < len(match) {
			continue
		}
		covered := true
		for _, c := range idx.Columns[:len(match)] {
			if !want[c] {
				covered = false
				break
			}
		}
		if covered {
			return true
		}
	}
	return false
}

// VerifyAgainstLive checks a declaration against the columns the database actually has.
//
// A mismatch is a configuration error and belongs at boot. Left to runtime it surfaces as a
// column-not-found from MySQL on whichever request happened to touch it, or — worse, for
// nullability — as a decode failure on whichever row happened to hold a NULL.
func VerifyAgainstLive(d *schema.Descriptor, live []LiveColumn) error {
	byName := make(map[string]LiveColumn, len(live))
	for _, c := range live {
		byName[c.Name] = c
	}

	for i := range d.Columns {
		col := &d.Columns[i]
		actual, ok := byName[col.Name]
		if !ok {
			return fmt.Errorf("storage: %s.%s is declared but the table does not have it", d.Name, col.Name)
		}
		if actual.Nullable != col.Nullable {
			return fmt.Errorf("storage: %s.%s is declared nullable=%t but the table has nullable=%t",
				d.Name, col.Name, col.Nullable, actual.Nullable)
		}
	}
	// Extra columns in the table are fine and expected — `updated_at` is one. They are simply not
	// cached, which the proxy already knows how to express by refusing to serve `SELECT *`.
	return nil
}
