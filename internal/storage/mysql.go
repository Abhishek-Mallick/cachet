package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ErrNotFound reports that a row does not exist.
//
// It is a sentinel because absence is a first-class, cacheable fact in Cachet — negative caching
// turns "this row does not exist" into a cache entry, and an insert must later invalidate it. A
// caller that cannot distinguish absence from an empty row cannot do that correctly.
var ErrNotFound = errors.New("storage: row not found")

// mysqlDuplicateEntry is ER_DUP_ENTRY.
const mysqlDuplicateEntry = 1062

// putRetries bounds how many times Put re-runs its transaction after losing an insert race.
const putRetries = 3

// Record is one row of the entities table.
type Record struct {
	ID       uint64
	TenantID uint32
	Status   uint8
	Payload  []byte

	// Version is the row's HLC version — the version of the write that last modified it. It is
	// maintained by Cachet, never by the application, and it is what every cache compare-and-set
	// is performed against (CONSISTENCY.md §2).
	Version Version
}

// Shard is the uncached data path for one database shard.
//
// It knows nothing about caching, which is the point: the baseline every benchmark in this project
// is measured against is exactly this type with nothing in front of it.
type Shard struct {
	id    ShardID
	db    *sql.DB
	clock *Clock
}

// OpenShard connects to a shard and verifies it is reachable.
//
// It fails fast rather than returning a lazily-connecting handle: a shard that is unreachable at
// boot is a configuration error, and discovering it on the first user request instead of at startup
// converts an operator problem into a customer problem.
func OpenShard(ctx context.Context, id ShardID, dsn string, clock *Clock) (*Shard, error) {
	if id == "" {
		return nil, errors.New("storage: empty shard id")
	}
	if clock == nil {
		return nil, errors.New("storage: nil clock")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open shard %s: %w", id, err)
	}

	// Bounded pool: an unbounded one converts a slow shard into thousands of queued connections and
	// turns a latency problem into an outage (CONTRIBUTING.md rule 3).
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(16)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping shard %s: %w", id, err)
	}
	return &Shard{id: id, db: db, clock: clock}, nil
}

// ID returns the shard's identifier.
func (s *Shard) ID() ShardID { return s.id }

// Close releases the shard's connection pool.
func (s *Shard) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close shard %s: %w", s.id, err)
	}
	return nil
}

// Get reads one row, returning the record and the fill version for a cache entry built from it.
//
// The fill version is sampled BEFORE the query is issued, and the direction of that choice is a
// correctness argument rather than a style preference. Sampling first can only UNDERSTATE how fresh
// the result is — a write landing mid-query is included in the result but not credited in the
// version — which costs an occasional extra cache miss. Sampling afterwards would OVERSTATE it: a
// write that committed just before the sample could be missing from the result while the entry
// claims to reflect it, and that is served staleness.
func (s *Shard) Get(ctx context.Context, id uint64) (Record, Version, error) {
	fill := s.clock.Now()

	const q = `SELECT id, tenant_id, status, payload, version FROM entities WHERE id = ?`

	var rec Record
	var version uint64
	err := s.db.QueryRowContext(ctx, q, id).Scan(&rec.ID, &rec.TenantID, &rec.Status, &rec.Payload, &version)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Record{}, fill, fmt.Errorf("get %d on %s: %w", id, s.id, ErrNotFound)
	case err != nil:
		return Record{}, 0, fmt.Errorf("get %d on %s: %w", id, s.id, err)
	}

	rec.Version = Version(version)
	// Adopting the row's version keeps this engine's clock from falling behind writes made by other
	// engines against the same shard. Without it, per-shard monotonicity is only per-process.
	s.clock.Observe(rec.Version)
	return rec, fill, nil
}

// BatchGet reads several rows in a single round trip, returning only the ones that exist.
//
// Absent ids are omitted rather than represented by zero values, so the caller distinguishes
// "missing" from "present but empty" by map membership — which is exactly what negative caching
// needs downstream.
func (s *Shard) BatchGet(ctx context.Context, ids []uint64) (map[uint64]Record, Version, error) {
	fill := s.clock.Now()
	if len(ids) == 0 {
		return map[uint64]Record{}, fill, nil
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	//nolint:gosec // G202: the interpolated fragment is a generated list of ? placeholders, never input.
	q := `SELECT id, tenant_id, status, payload, version FROM entities WHERE id IN (` + placeholders + `)`

	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("batch get on %s: %w", s.id, err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[uint64]Record, len(ids))
	for rows.Next() {
		var rec Record
		var version uint64
		if err := rows.Scan(&rec.ID, &rec.TenantID, &rec.Status, &rec.Payload, &version); err != nil {
			return nil, 0, fmt.Errorf("batch get scan on %s: %w", s.id, err)
		}
		rec.Version = Version(version)
		s.clock.Observe(rec.Version)
		out[rec.ID] = rec
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("batch get rows on %s: %w", s.id, err)
	}
	return out, fill, nil
}

// Put writes a row and returns the version it was stamped with.
//
// The transaction takes a row lock before stamping. That ordering is what makes versions monotonic
// per shard rather than merely per process: under the lock, this engine observes whatever version
// another engine last wrote and stamps strictly above it. Stamping before locking would let two
// engines with skewed clocks interleave inverted versions, and a stale cache fill would then win a
// compare-and-set — silent staleness, which is the one failure this project exists to eliminate.
func (s *Shard) Put(ctx context.Context, rec Record) (Version, error) {
	for attempt := 0; ; attempt++ {
		v, err := s.putOnce(ctx, rec)
		if err == nil {
			return v, nil
		}
		// A concurrent insert of the same absent row is the one contention case the row lock cannot
		// cover, because there is no row to lock yet. Retrying finds the row present and takes the
		// locked path.
		if attempt < putRetries && isDuplicateEntry(err) {
			continue
		}
		return 0, err
	}
}

func (s *Shard) putOnce(ctx context.Context, rec Record) (v Version, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("put %d on %s: begin: %w", rec.ID, s.id, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var existing uint64
	err = tx.QueryRowContext(ctx, `SELECT version FROM entities WHERE id = ? FOR UPDATE`, rec.ID).Scan(&existing)
	switch {
	case err == nil:
		s.clock.Observe(Version(existing))
	case errors.Is(err, sql.ErrNoRows):
		existing = 0
	default:
		return 0, fmt.Errorf("put %d on %s: lock: %w", rec.ID, s.id, err)
	}

	version := s.clock.Next()

	if existing == 0 {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, ?, ?, ?, ?)`,
			rec.ID, rec.TenantID, rec.Status, rec.Payload, uint64(version))
	} else {
		_, err = tx.ExecContext(ctx,
			`UPDATE entities SET tenant_id = ?, status = ?, payload = ?, version = ? WHERE id = ?`,
			rec.TenantID, rec.Status, rec.Payload, uint64(version), rec.ID)
	}
	if err != nil {
		return 0, fmt.Errorf("put %d on %s: write: %w", rec.ID, s.id, err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("put %d on %s: commit: %w", rec.ID, s.id, err)
	}
	return version, nil
}

// Delete removes a row and returns the version the removal was stamped with.
//
// The returned version is what the cache invalidation is stamped with, so it must outrank the write
// it supersedes — otherwise the tombstone loses its own compare-and-set and the deleted row stays
// readable from cache.
func (s *Shard) Delete(ctx context.Context, id uint64) (v Version, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("delete %d on %s: begin: %w", id, s.id, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var existing uint64
	err = tx.QueryRowContext(ctx, `SELECT version FROM entities WHERE id = ? FOR UPDATE`, id).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("delete %d on %s: %w", id, s.id, ErrNotFound)
	case err != nil:
		return 0, fmt.Errorf("delete %d on %s: lock: %w", id, s.id, err)
	}
	s.clock.Observe(Version(existing))

	version := s.clock.Next()
	if _, err = tx.ExecContext(ctx, `DELETE FROM entities WHERE id = ?`, id); err != nil {
		return 0, fmt.Errorf("delete %d on %s: %w", id, s.id, err)
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("delete %d on %s: commit: %w", id, s.id, err)
	}
	return version, nil
}

func isDuplicateEntry(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == mysqlDuplicateEntry
}
