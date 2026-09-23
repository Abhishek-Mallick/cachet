package storage

import (
	"context"
	"database/sql"
	"fmt"
)

// Introspect reads a table's real shape from INFORMATION_SCHEMA.
//
// Cachet declares its tables rather than discovering them — the system must not be the one choosing
// what is cacheable — but a declaration that has drifted from the database is a configuration error
// worth catching at boot. This is what turns "column not found" on a user request at 3am into a
// refusal to start with the column named.
func Introspect(ctx context.Context, db *sql.DB, table string) ([]LiveColumn, []Index, error) {
	cols, err := liveColumns(ctx, db, table)
	if err != nil {
		return nil, nil, err
	}
	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("storage: table %q does not exist in this schema", table)
	}
	idx, err := liveIndexes(ctx, db, table)
	if err != nil {
		return nil, nil, err
	}
	return cols, idx, nil
}

func liveColumns(ctx context.Context, db *sql.DB, table string) ([]LiveColumn, error) {
	// DATABASE() rather than a configured schema name: the connection already chose one, and
	// reading a different schema's metadata than the one the queries will run against is precisely
	// the mismatch this function exists to prevent.
	const q = `SELECT COLUMN_NAME, IS_NULLABLE
	           FROM INFORMATION_SCHEMA.COLUMNS
	           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
	           ORDER BY ORDINAL_POSITION`

	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, fmt.Errorf("storage: reading columns of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var out []LiveColumn
	for rows.Next() {
		var name, nullable string
		if err := rows.Scan(&name, &nullable); err != nil {
			return nil, fmt.Errorf("storage: reading columns of %q: %w", table, err)
		}
		out = append(out, LiveColumn{Name: name, Nullable: nullable == "YES"})
	}
	return out, rows.Err()
}

func liveIndexes(ctx context.Context, db *sql.DB, table string) ([]Index, error) {
	// Ordered by SEQ_IN_INDEX because a predicate is only cheap when it matches a LEADING prefix,
	// so the order of columns within an index is the whole point of reading them.
	const q = `SELECT INDEX_NAME, COLUMN_NAME
	           FROM INFORMATION_SCHEMA.STATISTICS
	           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
	           ORDER BY INDEX_NAME, SEQ_IN_INDEX`

	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, fmt.Errorf("storage: reading indexes of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	byName := map[string]*Index{}
	var order []string
	for rows.Next() {
		var idxName, colName string
		if err := rows.Scan(&idxName, &colName); err != nil {
			return nil, fmt.Errorf("storage: reading indexes of %q: %w", table, err)
		}
		if byName[idxName] == nil {
			byName[idxName] = &Index{Name: idxName}
			order = append(order, idxName)
		}
		byName[idxName].Columns = append(byName[idxName].Columns, colName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]Index, 0, len(order))
	for _, name := range order {
		out = append(out, *byName[name])
	}
	return out, nil
}
