package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// ColumnValue is one column and the value a conditional write compares it to or sets it to.
//
// A struct rather than a SQL fragment, on purpose. Accepting arbitrary SQL would make Cachet a
// query proxy, and a proxy cannot resolve affected keys — it can only guess at them from the text,
// which is the exact limitation the integrated model exists to escape. The column name is checked
// against a DECLARED predicate before anything runs, so no identifier ever reaches a statement.
type ColumnValue struct {
	Column string
	Value  schema.Value
}

// UpdateResult is what a conditional write did.
type UpdateResult struct {
	// Version is the version every matched row was stamped with, and the version the cache
	// invalidation must carry. It outranks every row it replaced, so the resulting tombstone cannot
	// lose its own compare-and-set to the value it supersedes.
	Version Version

	// Matched is how many rows the predicate hit — the blast radius, reported whether or not the
	// keys were resolved.
	Matched int

	// AffectedKeys is the exact set of rows touched, and is empty when Degraded is true.
	//
	// Empty rather than partial, deliberately. A partial list is worse than none: the caller would
	// invalidate what it was given and have no way to know which rows were silently left to CDC,
	// so it could not tell a complete invalidation from an incomplete one.
	AffectedKeys []schema.Key

	// Degraded reports that exact key resolution was abandoned because the predicate matched more
	// rows than the budget allowed. The WRITE still committed — degraded describes the
	// invalidation, never the durability (CONSISTENCY.md §5).
	Degraded bool
}

// ErrInvalidBudget is returned when a conditional write is given a non-positive key budget.
var ErrInvalidBudget = errors.New("storage: affected-key budget must be positive")

// ErrInvalidPredicate marks a conditional write the deployment did not declare, or one whose
// values do not line up with the shape it did declare. It is the caller's mistake, not the
// system's, and the two are reported differently.
var ErrInvalidPredicate = errors.New("storage: undeclared conditional write")

// UpdateWhere applies a declared conditional write and reports exactly which rows it touched.
//
// The key resolution happens INSIDE the transaction, with SELECT ... FOR UPDATE. That is the whole
// design and it is not an optimisation detail: resolving before the transaction would leave a
// window in which a row joins or leaves the predicate between the SELECT and the UPDATE. The write
// would then touch a row nobody invalidated, and the resulting staleness would present as a cache
// bug for as long as anyone cared to investigate it.
//
// Past maxAffected rows the resolution is abandoned rather than completed. Holding a transaction
// open across a million row locks to invalidate precisely is a worse outage than the staleness it
// prevents, so the write proceeds, reports degraded, and leaves those keys to the CDC tailer. What
// that costs is stated exactly in CONSISTENCY.md §5: the writer's own session guarantee survives,
// because the watermark still advances; other sessions' reads of those keys become
// BOUNDED(cdc_lag_bound) until Flux catches up.
func (s *Shard) UpdateWhere(ctx context.Context, table string, match, set []ColumnValue, maxAffected int) (res UpdateResult, err error) {
	if maxAffected <= 0 {
		// A zero budget would degrade every write, silently turning exact invalidation off across
		// the whole system. That is a configuration error, not a quiet mode change.
		return UpdateResult{}, fmt.Errorf("%w, got %d", ErrInvalidBudget, maxAffected)
	}

	t, err := s.Table(table)
	if err != nil {
		return UpdateResult{}, err
	}
	pred, err := t.Predicate(columnNames(match), columnNames(set))
	if err != nil {
		return UpdateResult{}, err
	}

	// Arguments follow the DECLARED column order, not the order the request wrote them in. The
	// statement was built from the declaration, so binding by request order would silently compare
	// one column's value against another's.
	matchArgs, err := argsInDeclaredOrder(pred.spec.Match, match)
	if err != nil {
		return UpdateResult{}, err
	}
	setArgs, err := argsInDeclaredOrder(pred.spec.Set, set)
	if err != nil {
		return UpdateResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("update where on %s: begin: %w", s.id, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	keys, err := s.resolveAffected(ctx, tx, t, pred, matchArgs, maxAffected)
	if err != nil {
		return UpdateResult{}, err
	}

	degraded := len(keys) > maxAffected
	version := s.clock.Next()

	// The version is the last assignment in every one of these statements, so it goes on the end of
	// the SET arguments and ahead of the WHERE's.
	write := append(append([]any{}, setArgs...), uint64(version))

	var affected int64
	switch {
	case degraded:
		// The budget was exceeded, so the keys gathered so far are an arbitrary prefix and useless
		// as an invalidation list. The write still applies to every matching row.
		result, execErr := tx.ExecContext(ctx, pred.UpdateStmt(), append(write, matchArgs...)...)
		if execErr != nil {
			err = fmt.Errorf("update where on %s: write: %w", s.id, execErr)
			return UpdateResult{}, err
		}
		affected, _ = result.RowsAffected()

	case len(keys) > 0:
		// Updating by the resolved keys rather than by the predicate again, so the rows written are
		// exactly the rows locked and reported. Re-evaluating the predicate could touch a row that
		// arrived after the SELECT — one more row nobody would invalidate.
		stmt, stmtErr := pred.UpdateByKeyStmt(len(keys))
		if stmtErr != nil {
			err = stmtErr
			return UpdateResult{}, err
		}
		args := write
		for _, k := range keys {
			args = append(args, k.Values[0])
		}
		result, execErr := tx.ExecContext(ctx, stmt, args...)
		if execErr != nil {
			err = fmt.Errorf("update where on %s: write: %w", s.id, execErr)
			return UpdateResult{}, err
		}
		affected, _ = result.RowsAffected()
	}

	if err = tx.Commit(); err != nil {
		return UpdateResult{}, fmt.Errorf("update where on %s: commit: %w", s.id, err)
	}

	res = UpdateResult{Version: version, Matched: int(affected), Degraded: degraded}
	if !degraded {
		res.AffectedKeys = keys
		res.Matched = len(keys)
	}
	return res, nil
}

// resolveAffected locks and returns the keys the predicate matches, plus at most one extra.
//
// One row over the budget is enough to know the budget was exceeded, and it stops a predicate
// matching a million rows from materialising a million keys just to count them.
func (s *Shard) resolveAffected(ctx context.Context, tx *sql.Tx, t *Table, pred *Predicate, matchArgs []any, maxAffected int) (keys []schema.Key, err error) {
	args := append(append([]any{}, matchArgs...), maxAffected+1)
	rows, err := tx.QueryContext(ctx, pred.ResolveStmt(), args...)
	if err != nil {
		return nil, fmt.Errorf("update where on %s: resolve: %w", s.id, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("update where on %s: resolve: %w", s.id, cerr)
			keys = nil
		}
	}()

	for rows.Next() {
		var rawKey, rawVersion []byte
		if err = rows.Scan(&rawKey, &rawVersion); err != nil {
			return nil, fmt.Errorf("update where on %s: scan: %w", s.id, err)
		}
		version, verr := schema.Bin(rawVersion).Uint64()
		if verr != nil {
			return nil, fmt.Errorf("update where on %s: %s: %w", s.id, t.d.VersionColumn.Name, verr)
		}
		// Every version seen advances the clock, so the version this write commits at cannot land
		// below a row it is about to overwrite.
		s.clock.Observe(Version(version))

		key, kerr := t.d.Key(string(rawKey))
		if kerr != nil {
			return nil, fmt.Errorf("update where on %s: %w", s.id, kerr)
		}
		keys = append(keys, key)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("update where on %s: resolve: %w", s.id, err)
	}
	return keys, nil
}

func columnNames(cv []ColumnValue) []string {
	out := make([]string, len(cv))
	for i, c := range cv {
		out[i] = c.Column
	}
	return out
}

// argsInDeclaredOrder lines the request's values up with the statement's placeholders.
func argsInDeclaredOrder(declared []string, given []ColumnValue) ([]any, error) {
	out := make([]any, len(declared))
	for i, name := range declared {
		found := false
		for _, g := range given {
			if g.Column != name {
				continue
			}
			if found {
				return nil, fmt.Errorf("%w: column %q appears twice in one conditional write", ErrInvalidPredicate, name)
			}
			out[i], found = g.Value.SQL(), true
		}
		if !found {
			return nil, fmt.Errorf("%w: no value given for %q", ErrInvalidPredicate, name)
		}
	}
	return out, nil
}
