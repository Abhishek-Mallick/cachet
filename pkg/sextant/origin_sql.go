package sextant

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
)

// Reading the system of record of a deployment that is not Cachet's.
//
// Two things are deliberately narrow. The statement is built once, at construction, from
// identifiers the caller supplied in configuration — nothing derived from a key ever becomes SQL.
// And the connection is used read-only in fact: a verifier that could write to what it checks would
// be able to influence the thing it reports on.

// SQLOriginOptions configures a verifier's view of a SQL system of record.
type SQLOriginOptions struct {
	// DSNs are the writers, in a stable order. One is the common case; several is a sharded
	// deployment, and then ShardFor decides which holds a key.
	DSNs []string

	// Table, KeyColumn and the columns below are identifiers, and they are the ONLY way an
	// identifier reaches a statement. They are validated at construction.
	Table     string
	KeyColumn string

	// VersionColumn is the column a version-tier comparison reads. Empty means this origin can only
	// be compared by value.
	VersionColumn string

	// ValueColumns is the projection a value-tier comparison compares against. It has to be
	// produced the same way the application produces what it caches, which is why it is declared
	// rather than guessed: a projection that does not match makes every entry look stale, forever.
	ValueColumns []string

	// ShardFor maps a key onto an index in DSNs. Nil means single-writer, which is what most
	// deployments are and what a verifier should not force anyone to describe.
	ShardFor func(key string) int
}

// SQLOrigin reads row state from one or more SQL writers.
type SQLOrigin struct {
	dbs      []*sql.DB
	names    []string
	shardFor func(key string) int

	stmt          string
	versionColumn bool
	valueColumns  int
}

// NewSQLOrigin opens the writers and builds the one statement this origin will ever issue.
func NewSQLOrigin(ctx context.Context, opts SQLOriginOptions) (*SQLOrigin, error) {
	if len(opts.DSNs) == 0 {
		return nil, errors.New("sextant: no origin DSNs")
	}
	if err := validIdentifiers(append([]string{opts.Table, opts.KeyColumn}, opts.ValueColumns...)); err != nil {
		return nil, err
	}
	if opts.VersionColumn != "" {
		if err := validIdentifiers([]string{opts.VersionColumn}); err != nil {
			return nil, err
		}
	}
	if opts.VersionColumn == "" && len(opts.ValueColumns) == 0 {
		return nil, errors.New("sextant: an origin must declare a version column, value columns, or both")
	}

	selected := make([]string, 0, len(opts.ValueColumns)+1)
	if opts.VersionColumn != "" {
		selected = append(selected, quoteIdentifier(opts.VersionColumn))
	}
	for _, c := range opts.ValueColumns {
		selected = append(selected, quoteIdentifier(c))
	}

	o := &SQLOrigin{
		shardFor:      opts.ShardFor,
		versionColumn: opts.VersionColumn != "",
		valueColumns:  len(opts.ValueColumns),
		stmt: "SELECT " + strings.Join(selected, ", ") +
			" FROM " + quoteIdentifier(opts.Table) +
			" WHERE " + quoteIdentifier(opts.KeyColumn) + " = ?",
	}
	if o.shardFor == nil {
		o.shardFor = func(string) int { return 0 }
	}

	for i, dsn := range opts.DSNs {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			_ = o.close()
			return nil, fmt.Errorf("sextant: open origin %d: %w", i, err)
		}
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			_ = o.close()
			return nil, fmt.Errorf("sextant: reach origin %d: %w", i, err)
		}
		// A verifier's load on the system it observes has to be a deliberate number. It samples; it
		// does not serve traffic.
		db.SetMaxOpenConns(4)
		db.SetMaxIdleConns(2)
		o.dbs = append(o.dbs, db)
		o.names = append(o.names, fmt.Sprintf("origin%d", i))
	}
	return o, nil
}

// Close releases the connections.
func (o *SQLOrigin) Close() error { return o.close() }

func (o *SQLOrigin) close() error {
	var err error
	for _, db := range o.dbs {
		if cerr := db.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// Statement is the one query this origin issues, for an operator who wants to see it before
// pointing a verifier at their database.
func (o *SQLOrigin) Statement() string { return o.stmt }

// State reads the row a key names.
//
// The key goes in as a BOUND ARGUMENT. Nothing derived from a key is ever concatenated into the
// statement — the whole statement was built at construction from configured identifiers — which is
// what makes a verifier safe to point at production.
func (o *SQLOrigin) State(ctx context.Context, key string) (State, bool, error) {
	db, _, err := o.dbFor(key)
	if err != nil {
		return State{}, false, err
	}

	dest := make([]any, 0, o.valueColumns+1)
	var version sql.Null[[]byte]
	if o.versionColumn {
		dest = append(dest, &version)
	}
	values := make([]sql.Null[[]byte], o.valueColumns)
	for i := range values {
		dest = append(dest, &values[i])
	}

	err = db.QueryRowContext(ctx, o.stmt, key).Scan(dest...)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return State{}, false, nil
	case err != nil:
		return State{}, false, fmt.Errorf("sextant: read %s: %w", key, err)
	}

	out := State{}
	if o.versionColumn && version.Valid {
		v, err := parseVersion(version.V)
		if err != nil {
			return State{}, false, fmt.Errorf("sextant: %s: version column: %w", key, err)
		}
		out.Version, out.VersionKnown = v, true
	}
	if o.valueColumns > 0 {
		out.Value = joinProjection(values)
	}
	return out, true, nil
}

// Shard names the writer a key belongs to, for attribution.
func (o *SQLOrigin) Shard(key string) (string, error) {
	_, name, err := o.dbFor(key)
	return name, err
}

func (o *SQLOrigin) dbFor(key string) (*sql.DB, string, error) {
	i := o.shardFor(key)
	if i < 0 || i >= len(o.dbs) {
		return nil, "", fmt.Errorf("sextant: key %q routed to writer %d, and %d are configured", key, i, len(o.dbs))
	}
	return o.dbs[i], o.names[i], nil
}

// HashShard routes keys across n writers by hashing the key.
//
// Offered because a deployment that shards by hash should not have to write Go to say so, and
// declared rather than defaulted because it is only correct if it is the SAME function the
// application uses. A verifier that guessed would read the wrong writer and report every key as
// missing — which looks like a healthy cache, since a missing row is nothing to be wrong about.
func HashShard(n int) func(string) int {
	return func(key string) int {
		if n <= 1 {
			return 0
		}
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		return int(h.Sum64() % uint64(n)) //nolint:gosec // bounded by the modulus
	}
}

// joinProjection renders the selected columns as the bytes a value comparison compares.
//
// NUL-separated, because a separator that can appear in a value would let two different rows
// produce the same projection — and a verifier that cannot tell two rows apart reports no
// violation for either.
func joinProjection(values []sql.Null[[]byte]) []byte {
	out := make([]byte, 0, 64)
	for i, v := range values {
		if i > 0 {
			out = append(out, 0)
		}
		if !v.Valid {
			// A NULL and an empty value must not render alike, for the same reason.
			out = append(out, 0xff)
			continue
		}
		out = append(out, v.V...)
	}
	return out
}

// parseVersion reads a version column's bytes as an unsigned integer.
//
// An error rather than a zero, because a version column that will not parse means the declaration
// does not match the database — and a zero would compare as "the oldest possible state", making
// every entry look fresh.
func parseVersion(b []byte) (uint64, error) {
	if len(b) == 0 {
		return 0, errors.New("empty value is not a version")
	}
	var u uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%q is not an unsigned integer", b)
		}
		next := u*10 + uint64(c-'0')
		if next < u {
			return 0, fmt.Errorf("%q overflows a uint64", b)
		}
		u = next
	}
	return u, nil
}
