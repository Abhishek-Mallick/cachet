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
	upsert   string
	del      string
	lockRow  string

	predicates []*Predicate

	// upsertAssigned is the column indexes the upsert's assignment list reassigns, in order.
	upsertAssigned []int
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

	// Collation is empty for non-character columns.
	Collation string
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

	// ON DUPLICATE KEY UPDATE rather than REPLACE: REPLACE deletes and re-inserts, which fires
	// delete triggers, churns the primary key index, and shows up in the binlog as two events the
	// CDC tailer would have to reason about.
	isKey := make(map[string]bool, len(d.PrimaryKey))
	for _, k := range d.PrimaryKey {
		isKey[k.Name] = true
	}
	dup := make([]string, 0, len(d.Columns))
	for i := range d.Columns {
		// Never reassign a primary key column: it is what the duplicate was detected on, and
		// setting it would move the row's identity out from under its cache entry.
		if isKey[d.Columns[i].Name] {
			continue
		}
		dup = append(dup, d.Columns[i].Quoted()+" = ?")
		t.upsertAssigned = append(t.upsertAssigned, i)
	}
	t.upsert = t.insert + " ON DUPLICATE KEY UPDATE " + strings.Join(dup, ", ")

	for _, p := range predicates {
		if err := t.validatePredicate(p, indexes); err != nil {
			return nil, err
		}
		built, err := t.buildPredicate(p)
		if err != nil {
			return nil, err
		}
		t.predicates = append(t.predicates, built)
	}
	return t, nil
}

// Predicate is one declared conditional-write shape, with its statements built.
//
// Built at boot from identifiers the schema package already validated, like every other statement
// here: no request ever contributes a column name, so there is nothing to escape at write time and
// nothing on the hot path concatenating SQL.
type Predicate struct {
	spec PredicateSpec

	// resolve locks the matching rows and reads their keys and versions.
	resolve string

	// update applies the write by the predicate, used only when the key resolution was abandoned.
	update string

	// updateByKey applies it to an explicit key list; the placeholder run is appended per call,
	// because its length is the only part that varies.
	updateByKey string
}

// Spec is the declaration this predicate was built from.
func (p *Predicate) Spec() PredicateSpec { return p.spec }

// ResolveStmt locks the rows the predicate matches, in key order.
func (p *Predicate) ResolveStmt() string { return p.resolve }

// UpdateStmt applies the write by the predicate itself.
func (p *Predicate) UpdateStmt() string { return p.update }

// UpdateByKeyStmt applies the write to exactly n resolved rows.
func (p *Predicate) UpdateByKeyStmt(n int) (string, error) {
	if n <= 0 {
		return "", fmt.Errorf("storage: update by key with %d keys", n)
	}
	return p.updateByKey + "(" + strings.TrimSuffix(strings.Repeat("?, ", n), ", ") + ")", nil
}

// Predicate returns the declared predicate matching and setting exactly these columns.
//
// Exact, not a superset. A request that matches on fewer columns than a declared predicate selects
// MORE rows than the deployment agreed to allow a single write to touch, and one that sets fewer
// changes something different. Neither is the shape that was validated against the table's indexes,
// so neither is served.
func (t *Table) Predicate(match, set []string) (*Predicate, error) {
	for _, p := range t.predicates {
		if sameColumns(p.spec.Match, match) && sameColumns(p.spec.Set, set) {
			return p, nil
		}
	}
	return nil, fmt.Errorf("%w: %s declares none matching on %v and setting %v",
		ErrInvalidPredicate, t.d.Name, match, set)
}

// Predicates returns every declared shape, in declaration order.
func (t *Table) Predicates() []*Predicate { return t.predicates }

// sameColumns compares two column sets, ignoring order.
//
// Ignoring order because a conjunction of equalities means the same thing however it is written,
// and requiring the declared order would refuse a request that is identical in effect.
func sameColumns(declared, requested []string) bool {
	if len(declared) != len(requested) {
		return false
	}
	seen := make(map[string]int, len(declared))
	for _, c := range declared {
		seen[c]++
	}
	for _, c := range requested {
		seen[c]--
		if seen[c] < 0 {
			return false
		}
	}
	return true
}

// buildPredicate renders the three statements one declared predicate needs.
func (t *Table) buildPredicate(spec PredicateSpec) (*Predicate, error) {
	// Resolution reads the primary key of every locked row and hands it back as a cache key, so a
	// composite key would have to be re-selected with a row constructor — which MyRocks plans
	// differently and which BatchGetStmt already refuses for the same reason. Refused at boot
	// rather than at the first conditional write.
	if len(t.d.PrimaryKey) != 1 {
		return nil, fmt.Errorf("storage: %s has a composite primary key; conditional writes are single-column only", t.d.Name)
	}

	match := make([]string, len(spec.Match))
	for i, name := range spec.Match {
		match[i] = t.d.Column(name).Quoted() + " = ?"
	}
	where := strings.Join(match, " AND ")

	// The version column is assigned by Cachet on every conditional write, last, so the stamped
	// version outranks every row the write replaces and the resulting tombstone cannot lose its
	// compare-and-set.
	assign := make([]string, 0, len(spec.Set)+1)
	for _, name := range spec.Set {
		assign = append(assign, t.d.Column(name).Quoted()+" = ?")
	}
	assign = append(assign, t.d.VersionColumn.Quoted()+" = ?")
	set := strings.Join(assign, ", ")

	pk := t.d.PrimaryKey[0].Quoted()
	return &Predicate{
		spec: spec,
		resolve: "SELECT " + pk + ", " + t.d.VersionColumn.Quoted() +
			" FROM " + t.d.QuotedName() + " WHERE " + where +
			" ORDER BY " + pk + " LIMIT ? FOR UPDATE",
		update:      "UPDATE " + t.d.QuotedName() + " SET " + set + " WHERE " + where,
		updateByKey: "UPDATE " + t.d.QuotedName() + " SET " + set + " WHERE " + pk + " IN ",
	}, nil
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

// UpsertStmt inserts a row, or replaces it if the primary key is taken.
//
// It takes the row's arguments TWICE: once for the VALUES list and once for the assignments.
func (t *Table) UpsertStmt() string { return t.upsert }

// UpsertAssignedColumns is the column indexes UpsertStmt reassigns, in the order it expects them.
func (t *Table) UpsertAssignedColumns() []int { return t.upsertAssigned }

// KeyOf builds the key naming a row.
func (t *Table) KeyOf(row []schema.Value) (schema.Key, error) {
	if len(row) != len(t.d.Columns) {
		return schema.Key{}, fmt.Errorf("storage: %s expects %d columns, got %d", t.d.Name, len(t.d.Columns), len(row))
	}
	values := make([]any, len(t.d.PrimaryKey))
	for i, col := range t.d.PrimaryKey {
		v := row[col.Index]
		if v.IsNull {
			return schema.Key{}, fmt.Errorf("storage: %s.%s is NULL and cannot name a row", t.d.Name, col.Name)
		}
		values[i] = string(v.Bytes)
	}
	return t.d.Key(values...)
}

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
		if err := checkKeyCollation(d, col, actual); err != nil {
			return err
		}
	}
	// Extra columns in the table are fine and expected — `updated_at` is one. They are simply not
	// cached, which the proxy expresses by refusing to serve `SELECT *`. UndeclaredColumns is how
	// it finds out.
	return nil
}

// checkKeyCollation verifies a string primary key column's DECLARED collation against the live one.
//
// The declaration alone is not enough. Under `utf8mb4_0900_ai_ci` MySQL treats 'Ann' and 'ann' as
// the SAME row while Go produces two cache keys, so an invalidation silently misses — the hazard
// ADR 0005 ranks first. A deployment declaring `utf8mb4_bin` over a `_ci` column would pass every
// config check and hit that hazard in production, which is why the two are compared here, against
// the database, at boot.
func checkKeyCollation(d *schema.Descriptor, col *schema.Column, actual LiveColumn) error {
	isKey := false
	for _, k := range d.PrimaryKey {
		if k.Name == col.Name {
			isKey = true
			break
		}
	}
	if !isKey || actual.Collation == "" {
		return nil
	}
	if !strings.EqualFold(col.Collation, actual.Collation) {
		return fmt.Errorf("storage: %s.%s is a primary key column declared with collation %q, "+
			"but the table has %q; two cache keys could name one row",
			d.Name, col.Name, col.Collation, actual.Collation)
	}
	return nil
}

// UndeclaredColumns returns the live table's columns that the descriptor does not declare.
//
// An empty result is a PROOF, established at boot against INFORMATION_SCHEMA, that a cache entry
// holds the whole row. That is what lets the proxy answer `SELECT *` instead of refusing it: with
// an undeclared column present, `*` could only be answered by inventing a value for it.
//
// Order follows the live table's, so a message listing them reads the way `DESCRIBE` does.
func UndeclaredColumns(d *schema.Descriptor, live []LiveColumn) []string {
	var out []string
	for _, c := range live {
		if d.Column(c.Name) == nil {
			out = append(out, c.Name)
		}
	}
	return out
}
