package storage

import (
	"context"
	"database/sql"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// DBForTest exposes the shard's connection so an introspection test can read INFORMATION_SCHEMA
// through the same connection the queries will use — which is the point, since Introspect scopes
// itself with DATABASE().
func DBForTest(s *Shard) *sql.DB { return s.db }

// ForceRowVersionForTest writes a row with an arbitrary version, bypassing the HLC.
//
// It lives in an export_test.go file so it is compiled only for tests and never reaches the public
// API — a production type must not carry a method that exists solely to let a test cheat.
//
// Its legitimate uses are simulating a second engine instance whose clock runs ahead, and
// simulating an out-of-band write made directly to MySQL. It must never be used to set up ordinary
// state; use PutRow for that.
func ForceRowVersionForTest(ctx context.Context, s *Shard, table string, row Row, v Version) error {
	t, err := s.Table(table)
	if err != nil {
		return err
	}
	stamped := make(Row, len(row))
	copy(stamped, row)
	stamped[t.d.VersionColumn.Index] = schema.Uint(uint64(v))

	args := make([]any, 0, 2*len(stamped))
	for i, val := range stamped {
		args = append(args, t.d.Columns[i].Arg(val))
	}
	for _, i := range t.UpsertAssignedColumns() {
		args = append(args, t.d.Columns[i].Arg(stamped[i]))
	}
	_, err = s.db.ExecContext(ctx, t.UpsertStmt(), args...)
	return err
}

// NormaliseDSNForTest exposes the connection-setting normalisation, which is otherwise observable
// only by connecting to a database and reading a DATETIME back.
func NormaliseDSNForTest(dsn string) (string, error) { return normaliseDSN(dsn) }
