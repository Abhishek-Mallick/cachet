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

// Table is the declared shape this shard serves, or nil if none is configured.
func (s *Shard) Table() *Table { return s.table }

// GetRow reads one row by primary key.
//
// The fill version is sampled BEFORE the query, for the same reason Get samples it first: sampling
// early can only understate freshness, which costs a cache miss, while sampling late can overstate
// it, which serves staleness.
func (s *Shard) GetRow(ctx context.Context, key schema.Key) (Row, Version, error) {
	fill := s.clock.Now()
	if s.table == nil {
		return nil, 0, fmt.Errorf("storage: %s has no table configured", s.id)
	}

	args, err := s.keyArgs(key)
	if err != nil {
		return nil, 0, err
	}

	row, err := scanRow(s.table, s.db.QueryRowContext(ctx, s.table.GetStmt(), args...))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, fill, fmt.Errorf("get %s on %s: %w", key, s.id, ErrNotFound)
	case err != nil:
		return nil, 0, fmt.Errorf("get %s on %s: %w", key, s.id, err)
	}

	v, err := s.rowVersion(row)
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
	if s.table == nil {
		return nil, 0, fmt.Errorf("storage: %s has no table configured", s.id)
	}

	stmt, err := s.table.BatchGetStmt(len(keys))
	if err != nil {
		return nil, 0, err
	}

	args := make([]any, 0, len(keys))
	for _, k := range keys {
		a, err := s.keyArgs(k)
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
		row, err := scanRows(s.table, rows)
		if err != nil {
			return nil, 0, fmt.Errorf("batch get on %s: %w", s.id, err)
		}
		key, err := s.table.KeyOf(row)
		if err != nil {
			return nil, 0, fmt.Errorf("batch get on %s: %w", s.id, err)
		}
		v, err := s.rowVersion(row)
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
func (s *Shard) PutRow(ctx context.Context, row Row) (v Version, err error) {
	if s.table == nil {
		return 0, fmt.Errorf("storage: %s has no table configured", s.id)
	}
	if len(row) != len(s.table.d.Columns) {
		return 0, fmt.Errorf("storage: %s expects %d columns, got %d", s.table.d.Name, len(s.table.d.Columns), len(row))
	}

	version := s.clock.Next()
	stamped := make(Row, len(row))
	copy(stamped, row)
	stamped[s.table.d.VersionColumn.Index] = schema.Uint(uint64(version))

	// The VALUES list takes every column; the assignment list takes every column the statement
	// actually reassigns, which excludes the primary key it matched on.
	args := make([]any, 0, 2*len(stamped))
	for _, val := range stamped {
		args = append(args, val.SQL())
	}
	for _, i := range s.table.UpsertAssignedColumns() {
		args = append(args, stamped[i].SQL())
	}

	// INSERT ... ON DUPLICATE KEY UPDATE rather than a read-then-write: the common case for a
	// cached table is a row that already exists, and a SELECT-then-write would need a transaction
	// to be correct under concurrency.
	if _, err := s.db.ExecContext(ctx, s.table.UpsertStmt(), args...); err != nil {
		return 0, fmt.Errorf("put on %s: %w", s.id, err)
	}
	return version, nil
}

// DeleteRow removes a row, returning the version the delete happened at.
func (s *Shard) DeleteRow(ctx context.Context, key schema.Key) (Version, error) {
	if s.table == nil {
		return 0, fmt.Errorf("storage: %s has no table configured", s.id)
	}
	args, err := s.keyArgs(key)
	if err != nil {
		return 0, err
	}

	version := s.clock.Next()
	res, err := s.db.ExecContext(ctx, s.table.DeleteStmt(), args...)
	if err != nil {
		return 0, fmt.Errorf("delete %s on %s: %w", key, s.id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("delete %s on %s: %w", key, s.id, err)
	}
	if n == 0 {
		return version, ErrNotFound
	}
	return version, nil
}

// keyArgs renders a key's values as query arguments, checking the arity against the descriptor.
func (s *Shard) keyArgs(key schema.Key) ([]any, error) {
	if len(key.Values) != len(s.table.d.PrimaryKey) {
		return nil, fmt.Errorf("storage: %s has %d primary key column(s), key %s has %d",
			s.table.d.Name, len(s.table.d.PrimaryKey), key, len(key.Values))
	}
	args := make([]any, len(key.Values))
	for i, v := range key.Values {
		args[i] = v
	}
	return args, nil
}

func (s *Shard) rowVersion(row Row) (Version, error) {
	u, err := row[s.table.d.VersionColumn.Index].Uint64()
	if err != nil {
		return 0, fmt.Errorf("storage: %s: reading the version column: %w", s.table.d.Name, err)
	}
	return Version(u), nil
}

// scanner is what sql.Row and sql.Rows have in common.
type scanner interface{ Scan(dest ...any) error }

func scanRow(t *Table, sc scanner) (Row, error)  { return scanInto(t, sc) }
func scanRows(t *Table, sc scanner) (Row, error) { return scanInto(t, sc) }

// scanInto reads a row as raw bytes.
//
// Every column is scanned into a *[]byte so that what comes back is what MySQL sent — which is what
// makes a cache hit byte-identical to a database read, and what lets the proxy answer over the wire
// without re-rendering. A NULL scans as a nil slice, which is how NULL is told from empty.
func scanInto(t *Table, sc scanner) (Row, error) {
	raw := make([][]byte, len(t.d.Columns))
	dest := make([]any, len(t.d.Columns))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := sc.Scan(dest...); err != nil {
		return nil, err
	}

	row := make(Row, len(raw))
	for i, b := range raw {
		if b == nil {
			row[i] = schema.Null()
			continue
		}
		// Copied: the driver may reuse its buffer for the next row.
		row[i] = schema.Bin(append([]byte(nil), b...))
	}
	return row, nil
}
