package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Predicate describes a conditional write: which rows to change, and what to change them to.
//
// It is a struct rather than a SQL fragment on purpose. Accepting arbitrary SQL would make Cachet a
// query proxy, and a proxy cannot resolve affected keys — it can only guess at them from the text,
// which is the exact limitation the integrated model exists to escape (product spec §4). A typed
// predicate is narrower and it is the reason the key list can be exact.
type Predicate struct {
	// TenantID and MatchStatus select the rows.
	TenantID    uint32
	MatchStatus uint8

	// SetStatus is the new status applied to every matched row.
	SetStatus uint8
}

// UpdateResult is what a conditional write did.
type UpdateResult struct {
	// Version is the HLC version every matched row was stamped with, and the version the cache
	// invalidation must be stamped with. It outranks every row it replaced, so the resulting
	// tombstone cannot lose its own compare-and-set to the value it supersedes.
	Version Version

	// Matched is how many rows the predicate hit — the blast radius, reported whether or not the
	// keys were resolved.
	Matched int

	// AffectedIDs is the exact set of rows touched, and is empty when Degraded is true.
	//
	// Empty rather than partial, deliberately. A partial list is worse than none: the caller would
	// invalidate what it was given and have no way to know which rows were silently left to CDC,
	// so it could not tell a complete invalidation from an incomplete one.
	AffectedIDs []uint64

	// Degraded reports that exact key resolution was abandoned because the predicate matched more
	// rows than the budget allowed. The WRITE still committed — degraded describes the
	// invalidation, never the durability (CONSISTENCY.md §5).
	Degraded bool
}

// ErrInvalidBudget is returned when a conditional write is given a non-positive key budget.
var ErrInvalidBudget = errors.New("storage: affected-key budget must be positive")

// UpdateWhere applies a conditional write and reports exactly which rows it touched.
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
func (s *Shard) UpdateWhere(ctx context.Context, p Predicate, maxAffected int) (res UpdateResult, err error) {
	if maxAffected <= 0 {
		// A zero budget would degrade every write, silently turning exact invalidation off across
		// the whole system. That is a configuration error, not a quiet mode change.
		return UpdateResult{}, fmt.Errorf("%w, got %d", ErrInvalidBudget, maxAffected)
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

	ids, err := s.resolveAffected(ctx, tx, p, maxAffected)
	if err != nil {
		return UpdateResult{}, err
	}

	degraded := len(ids) > maxAffected
	version := s.clock.Next()

	var affected int64
	if degraded {
		// The budget was exceeded, so the ids gathered so far are an arbitrary prefix and useless as
		// an invalidation list. The write still applies to every matching row.
		result, execErr := tx.ExecContext(ctx,
			`UPDATE entities SET status = ?, version = ? WHERE tenant_id = ? AND status = ?`,
			p.SetStatus, uint64(version), p.TenantID, p.MatchStatus)
		if execErr != nil {
			err = fmt.Errorf("update where on %s: write: %w", s.id, execErr)
			return UpdateResult{}, err
		}
		affected, _ = result.RowsAffected()
	} else if len(ids) > 0 {
		// Updating by the resolved ids rather than by the predicate again, so the rows written are
		// exactly the rows locked and reported. Re-evaluating the predicate could touch a row that
		// arrived after the SELECT — one more row nobody would invalidate.
		query, args := updateByIDs(ids, p.SetStatus, uint64(version))
		result, execErr := tx.ExecContext(ctx, query, args...)
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
		res.AffectedIDs = ids
		res.Matched = len(ids)
	}
	return res, nil
}

// resolveAffected locks and returns the ids the predicate matches, plus at most one extra.
//
// One row over the budget is enough to know the budget was exceeded, and it stops a predicate
// matching a million rows from materialising a million ids just to count them.
func (s *Shard) resolveAffected(ctx context.Context, tx *sql.Tx, p Predicate, maxAffected int) (ids []uint64, err error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT id, version FROM entities WHERE tenant_id = ? AND status = ? ORDER BY id LIMIT ? FOR UPDATE`,
		p.TenantID, p.MatchStatus, maxAffected+1)
	if err != nil {
		return nil, fmt.Errorf("update where on %s: resolve: %w", s.id, err)
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("update where on %s: resolve: %w", s.id, cerr)
			ids = nil
		}
	}()

	for rows.Next() {
		var id, version uint64
		if err = rows.Scan(&id, &version); err != nil {
			return nil, fmt.Errorf("update where on %s: scan: %w", s.id, err)
		}
		// Every version seen advances the clock, so the version this write commits at cannot land
		// below a row it is about to overwrite.
		s.clock.Observe(Version(version))
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("update where on %s: resolve: %w", s.id, err)
	}
	return ids, nil
}

// updateByIDs builds an UPDATE restricted to an explicit id list.
func updateByIDs(ids []uint64, status uint8, version uint64) (string, []any) {
	query := `UPDATE entities SET status = ?, version = ? WHERE id IN (?`
	args := make([]any, 0, len(ids)+2)
	args = append(args, status, version, ids[0])
	for _, id := range ids[1:] {
		query += `,?`
		args = append(args, id)
	}
	return query + `)`, args
}
