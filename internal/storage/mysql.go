package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	// Registers the "mysql" driver name that OpenShard passes to sql.Open. Blank because nothing
	// here calls into the package directly — and because dropping it turns every shard open into
	// "unknown driver", a long way from the import that caused it.
	_ "github.com/go-sql-driver/mysql"
)

// ErrNotFound reports that a row does not exist.
//
// It is a sentinel because absence is a first-class, cacheable fact in Cachet — negative caching
// turns "this row does not exist" into a cache entry, and an insert must later invalidate it. A
// caller that cannot distinguish absence from an empty row cannot do that correctly.
var ErrNotFound = errors.New("storage: row not found")

// Shard is the uncached data path for one database shard.
//
// It knows nothing about caching, which is the point: the baseline every benchmark in this project
// is measured against is exactly this type with nothing in front of it.
type Shard struct {
	id    ShardID
	db    *sql.DB
	clock *Clock

	// tables are the declared tables this shard serves, by name, and the source of every statement
	// issued against it. A shard opened for introspection has none yet, which is the order boot
	// has to happen in.
	tables map[string]*Table
}

// WithTables attaches declared tables to the shard.
//
// Separate from OpenShard so that the descriptors can come from configuration the caller has
// already validated, and so a shard opened for introspection can exist before anything has been
// declared against it — which is the order boot has to happen in: connect, read INFORMATION_SCHEMA,
// verify the declarations, then serve.
func (s *Shard) WithTables(tables ...*Table) *Shard {
	if s.tables == nil {
		s.tables = make(map[string]*Table, len(tables))
	}
	for _, t := range tables {
		s.tables[t.d.Name] = t
	}
	return s
}

// Introspect reads a table's live columns and indexes from this shard.
//
// On the shard rather than on a bare *sql.DB so that callers who need the schema at boot — the
// proxy proving its cached column set is the whole table, a descriptor being verified against the
// database — do not have to be handed the connection pool to get it.
func (s *Shard) Introspect(ctx context.Context, table string) ([]LiveColumn, []Index, error) {
	return Introspect(ctx, s.db, table)
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
