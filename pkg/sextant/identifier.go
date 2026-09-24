package sextant

import (
	"fmt"
	"regexp"
	"strings"
)

// Identifiers reach a statement from configuration and from nowhere else.
//
// Sextant is pointed at production databases it does not own, by people who are already nervous
// about that. The guarantee it offers in exchange is narrow and checkable: every identifier is
// validated against this pattern at construction, every statement is built once from validated
// identifiers, and every value is a bound argument. Nothing derived from a key becomes SQL.

var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,63}$`)

func validIdentifiers(names []string) error {
	for _, n := range names {
		if !identifierRe.MatchString(n) {
			return fmt.Errorf("sextant: %q is not a plain SQL identifier", n)
		}
	}
	return nil
}

// quoteIdentifier renders a validated identifier for SQL.
//
// Backtick-doubling as well as validation: the pattern already excludes a backtick, and quoting
// anyway means a future change to the pattern cannot silently become an injection.
func quoteIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
