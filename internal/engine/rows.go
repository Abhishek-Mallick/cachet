package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// The engine's own operations, in rows.
//
// Both wire protocols are adapters over this file. cachet.v1 spells one table's columns into the
// protocol and cachet.v2 carries a generic row, but neither one decides anything: routing, leases,
// admission, invalidation and every consistency rule live here, once. Two implementations of any of
// them would be two things that could come to mean different things by SESSION.

// RowResult is the answer to a read.
type RowResult struct {
	// Found is false when the row does not exist, which is an answer rather than an error: absence
	// is a cacheable fact.
	Found bool

	Row storage.Row

	// RowVersion orders writes against each other; FillVersion answers freshness. Both are carried
	// because they answer different questions (CONSISTENCY.md §1).
	RowVersion  storage.Version
	FillVersion storage.Version

	CacheHit bool

	// Entry is the cache entry that served the read, when CacheHit is true. It carries the
	// degradation metadata the response reports.
	Entry cache.Entry
}

// GetRow reads one row, from the cache when the requested level allows it.
func (e *Engine) GetRow(ctx context.Context, key schema.Key, reqmt consistency.Requirement, token *consistency.Token) (RowResult, error) {
	t, err := e.tableFor(key)
	if err != nil {
		return RowResult{}, err
	}
	shard, id, err := t.shardFor(key.String())
	if err != nil {
		return RowResult{}, err
	}

	entry, served, lease := e.fromCacheOrLease(ctx, reqmt, key.String(), id, token)
	if served {
		token.Advance(string(id), entry.RowVersion)
		row, err := t.d.DecodeRow(entry.Row)
		if err != nil && !entry.Negative {
			// Unreachable on a healthy entry: the Lua returns only rows whose fingerprint matches
			// this build's, so the shape is known before the bytes are read. Falling through to the
			// database is the conservative answer if it ever happens — wrong in the direction of a
			// miss rather than of inventing column values.
			e.log.WarnContext(ctx, "cached row did not decode; reading the origin", "key", key, "err", err)
		} else {
			return RowResult{
				Found:       !entry.Negative,
				Row:         row,
				RowVersion:  storage.Version(entry.RowVersion),
				FillVersion: storage.Version(entry.FillVersion),
				CacheHit:    true,
				Entry:       entry,
			}, nil
		}
	}

	row, fill, err := shard.GetRow(ctx, key)
	e.metrics.RecordOriginRead()
	switch {
	case errors.Is(err, storage.ErrNotFound):
		// Absence is an answer, not an error: "this row does not exist" is a cacheable fact, and an
		// insert must later invalidate that negative entry.
		//
		// Nothing advances here: an absent row has no version, so there is no version a later read
		// of this key could move backwards from. Read-own-inserts is carried by the INSERT
		// advancing the watermark, not by this read.
		e.fillNegativeHoldingLease(ctx, key.String(), fill, lease)
		return RowResult{FillVersion: fill}, nil
	case err != nil:
		return RowResult{}, err
	}

	version, err := rowVersion(t.d, row)
	if err != nil {
		return RowResult{}, err
	}

	// Observing advances the watermark, which is what gives monotonic reads without any extra
	// state (CONSISTENCY.md §3.2).
	//
	// By the ROW's version, not the read's fill version. The guarantee is scoped to a key —
	// "successive reads of k never move backwards in version" — and a fill version is "when we
	// looked", which has nothing to do with k. Advancing by the fill version implements a far
	// stronger rule, that no entry filled before the newest fill this session has seen on this
	// shard may be served, and that rule costs the entire hit rate: every read ratchets the
	// watermark past every other key's entry, so a session reading more than one key on a shard
	// never gets a hit again.
	//
	// The row version is enough. An entry is served only when its FILL version is at or after the
	// watermark, and an entry filled after the newest row version this session has observed cannot
	// be hiding a write the session has already seen.
	token.Advance(string(id), uint64(version))
	e.fillRowHoldingLease(ctx, t, key.String(), row, version, fill, lease)

	return RowResult{Found: true, Row: row, RowVersion: version, FillVersion: fill}, nil
}

// BatchGetRows reads several rows, one query per shard rather than one per key.
//
// There is deliberately no cross-key snapshot at any level: Cachet caches rows, not transactions.
// Offering one here would invite the assumption that it holds across shards, where it cannot
// (CONSISTENCY.md §6).
func (e *Engine) BatchGetRows(ctx context.Context, keys []schema.Key, token *consistency.Token) (map[string]storage.Row, storage.Version, error) {
	// Grouped by table first, then by shard. Each table has its own ring, so one ring's grouping
	// would place another table's keys wherever that ring happened to put them.
	byTable := make(map[string][]schema.Key, 1)
	for _, k := range keys {
		if _, err := e.tableFor(k); err != nil {
			return nil, 0, err
		}
		byTable[k.Table] = append(byTable[k.Table], k)
	}

	out := make(map[string]storage.Row, len(keys))
	var newest storage.Version

	for name, tableKeys := range byTable {
		t := e.tables[name]

		raw := make([]string, 0, len(tableKeys))
		byString := make(map[string]schema.Key, len(tableKeys))
		for _, k := range tableKeys {
			raw = append(raw, k.String())
			byString[k.String()] = k
		}
		groups, err := t.router.Group(raw)
		if err != nil {
			return nil, 0, err
		}

		for shardID, shardKeys := range groups {
			resolved := make([]schema.Key, 0, len(shardKeys))
			for _, k := range shardKeys {
				resolved = append(resolved, byString[k])
			}

			rows, fill, err := t.shards[shardID].BatchGetRows(ctx, resolved)
			if err != nil {
				return nil, 0, err
			}
			if fill > newest {
				newest = fill
			}

			for _, k := range shardKeys {
				row, found := rows[k]
				if !found {
					e.fillNegative(ctx, k, fill)
					continue
				}
				version, err := rowVersion(t.d, row)
				if err != nil {
					return nil, 0, err
				}
				// The newest ROW version in this batch, for the reason given in GetRow.
				token.Advance(string(shardID), uint64(version))
				out[k] = row
				e.fillRow(ctx, t, k, row, version, fill)
			}
		}
	}
	return out, newest, nil
}

// PutRow writes one row and advances the caller's session watermark to the committed version.
//
// The watermark advance is what makes read-own-writes possible at all: the client carries it
// forward, and a later read rejects any cache entry filled before this write. Returning it is
// therefore part of the write's contract, not a convenience.
func (e *Engine) PutRow(ctx context.Context, key schema.Key, row storage.Row, token *consistency.Token) (storage.Version, error) {
	t, err := e.tableFor(key)
	if err != nil {
		return 0, err
	}
	if len(row) != len(t.d.Columns) {
		return 0, fmt.Errorf("%w: %s has %d columns, got %d",
			ErrInvalidRequest, t.d.Name, len(t.d.Columns), len(row))
	}

	// The key and the row's primary key columns have to agree. They arrive as separate fields, so
	// a caller that built one from a stale copy of the other would write a row under a key that
	// does not name it — a row findable only by a key nobody would construct.
	//
	// Refused rather than reconciled. Overwriting the row's columns from the key would accept the
	// request and silently discard what the caller sent, which is the same corruption wearing a
	// success code.
	for i, col := range t.d.PrimaryKey {
		if v := row[col.Index]; v.IsNull || string(v.Bytes) != key.Values[i] {
			return 0, fmt.Errorf("%w: key %s does not match the row's %s column, which is %s",
				ErrInvalidRequest, key, col.Name, v)
		}
	}

	shard, id, err := t.shardFor(key.String())
	if err != nil {
		return 0, err
	}
	version, err := shard.PutRow(ctx, t.d.Name, row)
	if err != nil {
		return 0, err
	}

	// After the commit, before the ack. By the time the caller holds this response, the stale entry
	// is already tombstoned at this version — which is what lets a DIFFERENT process, handed this
	// session token, read the write.
	e.invalidate(ctx, key.String(), version)
	token.Advance(string(id), uint64(version))
	return version, nil
}

// DeleteRow removes one row. The boolean reports whether it existed, which is not an error either
// way: it determines whether a negative cache entry needed invalidating.
func (e *Engine) DeleteRow(ctx context.Context, key schema.Key, token *consistency.Token) (bool, storage.Version, error) {
	t, err := e.tableFor(key)
	if err != nil {
		return false, 0, err
	}
	shard, id, err := t.shardFor(key.String())
	if err != nil {
		return false, 0, err
	}

	version, err := shard.DeleteRow(ctx, key)
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return false, 0, nil
	case err != nil:
		return false, 0, err
	}

	// A delete invalidates exactly like an update: the tombstone carries the delete's version, so a
	// read that started earlier cannot refill the row it removed.
	e.invalidate(ctx, key.String(), version)
	token.Advance(string(id), uint64(version))
	return true, version, nil
}

// UpdateOutcome is what a conditional write did across every shard it ran on.
type UpdateOutcome struct {
	Matched uint64

	// AffectedKeys is exact when Degraded is false and EMPTY when it is true — never partial.
	AffectedKeys []string

	Degraded bool

	// Newest is the highest version any shard stamped. It is reported for logs and debugging only:
	// versions from different shards are incomparable (ADR 0003), so a single scalar cannot
	// describe a write that touched several. The SESSION TOKEN is authoritative.
	Newest storage.Version
}

// UpdateRowsWhere applies a declared conditional write across every shard the table lives on.
//
// This is the Tier 1 capability the whole architecture is for. A proxy sitting outside the database
// sees `UPDATE t SET status=? WHERE tenant_id=?` and can only infer which rows that touched; an
// engine that owns the read path resolves them exactly, inside the transaction, and invalidates
// them before the write is acknowledged (product spec §4).
//
// The predicate is not key-scoped, so it runs on every shard. Each shard resolves and commits
// independently at its own HLC version — versions from different shards are incomparable (ADR
// 0003), which is why the session token is a per-shard map rather than a scalar.
func (e *Engine) UpdateRowsWhere(ctx context.Context, tableName string, match, set []storage.ColumnValue, token *consistency.Token) (UpdateOutcome, error) {
	t, ok := e.tables[tableName]
	if !ok {
		return UpdateOutcome{}, fmt.Errorf("%w: no table named %q is served by this engine", ErrInvalidRequest, tableName)
	}

	var out UpdateOutcome
	for _, shardID := range t.router.Shards() {
		shard, ok := t.shards[shardID]
		if !ok {
			return UpdateOutcome{}, fmt.Errorf("engine: no open shard %s", shardID)
		}

		res, err := shard.UpdateWhere(ctx, tableName, match, set, e.maxAffectedKeys)
		switch {
		case errors.Is(err, storage.ErrInvalidPredicate):
			// A shape the deployment did not declare. The caller's mistake, and one they can only
			// fix if they are told which shape was asked for.
			return UpdateOutcome{}, errors.Join(ErrInvalidRequest, err)
		case err != nil:
			return UpdateOutcome{}, err
		}
		if res.Matched > 0 {
			out.Matched += uint64(res.Matched)
		}

		// The watermark advances on every shard the write touched, degraded or not. This is what
		// keeps the WRITER's own read-own-writes guarantee intact when exact invalidation was given
		// up: its later reads reject any entry filled from a database state older than this write,
		// with no invalidation involved at all (CONSISTENCY.md §5).
		if res.Version > 0 {
			token.Advance(string(shardID), uint64(res.Version))
			if res.Version > out.Newest {
				out.Newest = res.Version
			}
		}

		if res.Degraded {
			out.Degraded = true
			continue
		}
		for _, key := range res.AffectedKeys {
			// After the commit, before the ack — the same ordering PutRow relies on, and what lets
			// a different session see the change immediately.
			e.invalidate(ctx, key.String(), res.Version)
			out.AffectedKeys = append(out.AffectedKeys, key.String())
		}
	}

	if out.Degraded {
		// Reported empty, never partial — even though the shards that stayed under budget were
		// invalidated exactly and that work is kept. The list is a CONTRACT: complete when degraded
		// is false, absent when it is true. Handing back a partial list would leave the caller
		// unable to distinguish the rows it must treat as BOUNDED(cdc_lag_bound) from the rows
		// already invalidated, which is the one thing it needs this field for.
		out.AffectedKeys = nil
	}
	return out, nil
}

// fillRow caches a row read from the origin.
func (e *Engine) fillRow(ctx context.Context, t *table, key string, row storage.Row, version, fillVersion storage.Version) {
	e.fillRowHoldingLease(ctx, t, key, row, version, fillVersion, "")
}

// fillRowHoldingLease is fillRow by a caller that was granted the lease for this key, which the
// fill hands back. An empty token means no lease was held.
func (e *Engine) fillRowHoldingLease(ctx context.Context, t *table, key string, row storage.Row, version, fillVersion storage.Version, lease string) {
	if e.cache == nil {
		return
	}
	encoded, err := t.d.EncodeRow(row)
	if err != nil {
		// A row that will not encode must not be cached: the next reader would get a decode error
		// where a database read would have worked. Logged and skipped, so the read still succeeds.
		e.log.WarnContext(ctx, "cache fill skipped; the row did not encode", "key", key, "err", err)
		return
	}
	e.applyFillHoldingLease(ctx, key, cache.Entry{
		RowVersion:  uint64(version),
		FillVersion: uint64(fillVersion),
		Row:         encoded,
	}, lease)
}

// rowVersion reads a row's version column.
func rowVersion(d *schema.Descriptor, row storage.Row) (storage.Version, error) {
	if len(row) != len(d.Columns) {
		return 0, fmt.Errorf("engine: %s has %d columns, got %d", d.Name, len(d.Columns), len(row))
	}
	v, err := row[d.VersionColumn.Index].Uint64()
	if err != nil {
		return 0, fmt.Errorf("engine: %s.%s: %w", d.Name, d.VersionColumn.Name, err)
	}
	return storage.Version(v), nil
}
