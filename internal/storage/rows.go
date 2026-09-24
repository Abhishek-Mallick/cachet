package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// Row is one row of a declared table, in the descriptor's column order.
type Row []schema.Value

// Table returns one declared table this shard serves.
//
// An error rather than a nil table: a shard asked for a table it was not given is a routing or
// configuration mistake, and returning nil would turn it into a panic somewhere downstream with no
// mention of which table was missing.
func (s *Shard) Table(name string) (*Table, error) {
	t, ok := s.tables[name]
	if !ok {
		return nil, fmt.Errorf("storage: shard %s serves no table named %q", s.id, name)
	}
	return t, nil
}

// Tables returns every declared table this shard serves.
func (s *Shard) Tables() map[string]*Table { return s.tables }

// GetRow reads one row by primary key.
//
// The fill version is sampled BEFORE the query, for the same reason Get samples it first: sampling
// early can only understate freshness, which costs a cache miss, while sampling late can overstate
// it, which serves staleness.
func (s *Shard) GetRow(ctx context.Context, key schema.Key) (Row, Version, error) {
	fill := s.clock.Now()
	t, err := s.Table(key.Table)
	if err != nil {
		return nil, 0, err
	}

	args, err := t.keyArgs(key)
	if err != nil {
		return nil, 0, err
	}

	row, err := scanRow(t, s.db.QueryRowContext(ctx, t.GetStmt(), args...))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fill, fmt.Errorf("get %s on %s: %w", key, s.id, ErrNotFound)
	case err != nil:
		return nil, 0, fmt.Errorf("get %s on %s: %w", key, s.id, err)
	}

	v, err := t.rowVersion(row)
	if err != nil {
		return nil, 0, err
	}
	// Adopting the row's version keeps this engine's clock from falling behind writes made by other
	// engines against the same shard. Without it, per-shard monotonicity is only per-process.
	s.clock.Observe(v)
	return row, fill, nil
}

// BatchGetRows reads several rows in one round trip, returning only the ones that exist.
//
// Absent keys are omitted rather than represented by an empty row, so a caller distinguishes
// "missing" from "present but empty" by map membership — which is what negative caching needs.
func (s *Shard) BatchGetRows(ctx context.Context, keys []schema.Key) (map[string]Row, Version, error) {
	fill := s.clock.Now()
	if len(keys) == 0 {
		return map[string]Row{}, fill, nil
	}

	// One statement, so one table. A batch spanning two tables would need two queries and would
	// hide that from the caller, who is the only one who can decide whether that is worth a second
	// round trip; the engine groups by table before it gets here.
	t, err := s.Table(keys[0].Table)
	if err != nil {
		return nil, 0, err
	}

	stmt, err := t.BatchGetStmt(len(keys))
	if err != nil {
		return nil, 0, err
	}

	args := make([]any, 0, len(keys))
	for _, k := range keys {
		if k.Table != t.d.Name {
			return nil, 0, fmt.Errorf("storage: batch on %s mixes tables %q and %q", s.id, t.d.Name, k.Table)
		}
		a, err := t.keyArgs(k)
		if err != nil {
			return nil, 0, err
		}
		args = append(args, a...)
	}

	rows, err := s.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("batch get on %s: %w", s.id, err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]Row, len(keys))
	for rows.Next() {
		row, err := scanRows(t, rows)
		if err != nil {
			return nil, 0, fmt.Errorf("batch get on %s: %w", s.id, err)
		}
		key, err := t.KeyOf(row)
		if err != nil {
			return nil, 0, fmt.Errorf("batch get on %s: %w", s.id, err)
		}
		v, err := t.rowVersion(row)
		if err != nil {
			return nil, 0, err
		}
		s.clock.Observe(v)
		out[key.String()] = row
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("batch get on %s: %w", s.id, err)
	}
	return out, fill, nil
}

// PutRow inserts or replaces a row, stamping it with a fresh version.
//
// The version this shard issues replaces whatever the caller put in that column: versions are
// Cachet's to hand out, and a write carrying its own would order itself against the engine's.
func (s *Shard) PutRow(ctx context.Context, table string, row Row) (v Version, err error) {
	t, err := s.Table(table)
	if err != nil {
		return 0, err
	}
	if len(row) != len(t.d.Columns) {
		return 0, fmt.Errorf("storage: %s expects %d columns, got %d", t.d.Name, len(t.d.Columns), len(row))
	}

	key, err := t.KeyOf(row)
	if err != nil {
		return 0, err
	}

	// In a transaction, behind a locking read of the row's current version.
	//
	// The read is not about the value — nothing here uses the old row. It is about the CLOCK. A
	// second engine whose wall clock runs ahead may have written this row at a version far in the
	// future; stamping ours from an unadjusted clock would produce a LOWER version for a LATER
	// write, and per-shard monotonicity would break the moment a second engine joined. A stale
	// fill would then win a compare-and-set against a fresh one (ADR 0003).
	//
	// FOR UPDATE rather than a plain read, because between observing and stamping, another writer
	// doing the same thing would otherwise interleave and both would stamp from the same
	// observation.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("put %s on %s: begin: %w", key, s.id, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = s.observeRowVersion(ctx, tx, t, key); err != nil {
		return 0, err
	}

	version := s.clock.Next()
	stamped := make(Row, len(row))
	copy(stamped, row)
	stamped[t.d.VersionColumn.Index] = schema.Uint(uint64(version))

	// The VALUES list takes every column; the assignment list takes every column the statement
	// actually reassigns, which excludes the primary key it matched on.
	// Bound per COLUMN, not per value: the column's type decides whether the driver is handed a
	// string or bytes, and MySQL refuses to build a JSON value out of bytes.
	args := make([]any, 0, 2*len(stamped))
	for i, val := range stamped {
		args = append(args, t.d.Columns[i].Arg(val))
	}
	for _, i := range t.UpsertAssignedColumns() {
		args = append(args, t.d.Columns[i].Arg(stamped[i]))
	}

	// INSERT ... ON DUPLICATE KEY UPDATE rather than an INSERT-or-UPDATE decision: the row may
	// have been created by someone else between the locking read finding nothing and this write,
	// and a race that resolves itself is better than one that returns a duplicate-key error.
	if _, err = tx.ExecContext(ctx, t.UpsertStmt(), args...); err != nil {
		return 0, fmt.Errorf("put %s on %s: %w", key, s.id, err)
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("put %s on %s: commit: %w", key, s.id, err)
	}
	return version, nil
}

// observeRowVersion locks a row and folds its version into this shard's clock.
//
// A missing row is not an error: there is no version to adopt, and the write that follows is an
// insert.
func (s *Shard) observeRowVersion(ctx context.Context, tx *sql.Tx, t *Table, key schema.Key) error {
	args, err := t.keyArgs(key)
	if err != nil {
		return err
	}

	var raw []byte
	err = tx.QueryRowContext(ctx, t.LockRowStmt(), args...).Scan(&raw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("lock %s on %s: %w", key, s.id, err)
	}

	existing, err := schema.Bin(raw).Uint64()
	if err != nil {
		return fmt.Errorf("lock %s on %s: %s: %w", key, s.id, t.d.VersionColumn.Name, err)
	}
	s.clock.Observe(Version(existing))
	return nil
}

// DeleteRow removes a row, returning the version the delete happened at.
func (s *Shard) DeleteRow(ctx context.Context, key schema.Key) (Version, error) {
	t, err := s.Table(key.Table)
	if err != nil {
		return 0, err
	}
	args, err := t.keyArgs(key)
	if err != nil {
		return 0, err
	}

	// The same locking read Put does, for the same reason: the tombstone this delete produces must
	// outrank the row it removes, including a row written by an engine whose clock ran ahead. A
	// tombstone that loses its compare-and-set leaves the deleted row served from cache.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("delete %s on %s: begin: %w", key, s.id, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = s.observeRowVersion(ctx, tx, t, key); err != nil {
		return 0, err
	}

	version := s.clock.Next()
	res, err := tx.ExecContext(ctx, t.DeleteStmt(), args...)
	if err != nil {
		return 0, fmt.Errorf("delete %s on %s: %w", key, s.id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("delete %s on %s: %w", key, s.id, err)
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("delete %s on %s: commit: %w", key, s.id, err)
	}
	if n == 0 {
		return version, ErrNotFound
	}
	return version, nil
}

// keyArgs renders a key's values as query arguments, checking the arity against the descriptor.
func (t *Table) keyArgs(key schema.Key) ([]any, error) {
	if key.Table != t.d.Name {
		return nil, fmt.Errorf("storage: key %s does not name table %q", key, t.d.Name)
	}
	if len(key.Values) != len(t.d.PrimaryKey) {
		return nil, fmt.Errorf("storage: %s has %d primary key column(s), key %s has %d",
			t.d.Name, len(t.d.PrimaryKey), key, len(key.Values))
	}
	args := make([]any, len(key.Values))
	for i, v := range key.Values {
		args[i] = v
	}
	return args, nil
}

func (t *Table) rowVersion(row Row) (Version, error) {
	u, err := row[t.d.VersionColumn.Index].Uint64()
	if err != nil {
		return 0, fmt.Errorf("storage: %s: reading the version column: %w", t.d.Name, err)
	}
	return Version(u), nil
}

// scanner is what sql.Row and sql.Rows have in common.
type scanner interface{ Scan(dest ...any) error }

func scanRow(t *Table, sc scanner) (Row, error)  { return scanInto(t, sc) }
func scanRows(t *Table, sc scanner) (Row, error) { return scanInto(t, sc) }

// scanInto reads a row as raw bytes.
//
// Every column is scanned as bytes so that what comes back is what MySQL sent — which is what makes
// a cache hit byte-identical to a database read, and what lets the proxy answer over the wire
// without re-rendering.
//
// sql.Null[[]byte] rather than a bare *[]byte, because a bare one cannot tell NULL from the empty
// string: the driver hands back a nil slice for both, and a `note` column containing ” would come
// back as SQL NULL. Those are different facts — the whole reason the row encoding carries a null
// bitmap instead of treating absent bytes as absent values — so the distinction has to survive the
// scan, not just the encoding.
func scanInto(t *Table, sc scanner) (Row, error) {
	raw := make([]sql.Null[[]byte], len(t.d.Columns))
	dest := make([]any, len(t.d.Columns))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := sc.Scan(dest...); err != nil {
		return nil, err
	}

	row := make(Row, len(raw))
	for i, v := range raw {
		if !v.Valid {
			row[i] = schema.Null()
			continue
		}
		// Copied, and never nil: the driver may reuse its buffer for the next row, and a nil slice
		// here would be indistinguishable from NULL to everything downstream.
		row[i] = schema.Bin(append([]byte{}, v.V...))
	}
	return row, nil
}
