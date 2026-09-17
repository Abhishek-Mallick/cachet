package storage

import (
	"fmt"
	"strings"
)

// ParseDSN splits a MySQL DSN into the parts a replication client needs.
//
// It lives here rather than in a binary because two of them need it — Flux to invalidate and
// Sextant to observe — and a second copy would be a second chance to disagree about what a DSN
// means. The shard configuration is already this package's concern, so the parser belongs with it.
//
// Deliberately narrow: it understands the form Cachet's own configuration uses
// (user:password@tcp(host:port)/database?params) rather than every DSN MySQL will accept. A parser
// that silently mis-read an exotic DSN would connect a replication client somewhere unexpected,
// which is a worse failure than refusing it.
func ParseDSN(dsn string) (addr, user, password, database string, err error) {
	creds, rest, ok := strings.Cut(dsn, "@")
	if !ok {
		return "", "", "", "", fmt.Errorf("storage: malformed dsn %q", dsn)
	}
	user, password, _ = strings.Cut(creds, ":")

	_, rest, ok = strings.Cut(rest, "(")
	if !ok {
		return "", "", "", "", fmt.Errorf("storage: dsn %q has no host", dsn)
	}
	addr, rest, ok = strings.Cut(rest, ")")
	if !ok {
		return "", "", "", "", fmt.Errorf("storage: dsn %q has no host", dsn)
	}

	database = strings.TrimPrefix(rest, "/")
	if i := strings.IndexByte(database, '?'); i >= 0 {
		database = database[:i]
	}
	if database == "" {
		return "", "", "", "", fmt.Errorf("storage: dsn %q has no database", dsn)
	}
	return addr, user, password, database, nil
}
