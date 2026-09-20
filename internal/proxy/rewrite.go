package proxy

import (
	"regexp"
	"strings"
)

// rewriteWithVersionBump adds `version = version + 1` to a single-row UPDATE.
//
// This is the most dangerous operation in the package: the result is executed against the caller's
// database. So it finds its splice point with a quote-aware scan rather than a regular expression,
// and refuses anything the scan cannot resolve.
//
// It exists because Cachet's invalidation is a versioned compare-and-set. A tombstone carrying a
// version no newer than the cached entry is REJECTED, so a raw SQL update that leaves the version
// column alone would leave a stale entry that nothing ever clears — not the write path, which never
// saw it, and not the CDC tailer, whose tombstone would carry the unchanged version and lose the
// compare-and-set. Bumping it here keeps the discipline the SDK keeps, without asking the
// application to know it exists.
//
// `version + 1` rather than a clock reading: it needs no coordination with the engine's clock and
// is strictly greater than whatever the cached entry holds, which is all the compare-and-set needs.
func rewriteWithVersionBump(query string) (string, bool) {
	q := strings.TrimSpace(query)
	trimmed := strings.TrimSuffix(q, ";")

	// A delete removes the row, so there is no version left to carry. The tombstone still uses a
	// version newer than the row held, which the caller computes.
	if deletePrefixRe.MatchString(trimmed) {
		return q, true
	}
	if !updatePrefixRe.MatchString(trimmed) {
		return "", false
	}

	where, ok := topLevelWhere(trimmed)
	if !ok {
		return "", false
	}

	sets := trimmed[:where]
	// Already maintained by the application: leave it exactly as written rather than assigning the
	// column twice.
	if versionAssignRe.MatchString(sets) {
		return q, true
	}

	// Backticked so the splice cannot collide with a column of the same name in a different case
	// or quoting style.
	return strings.TrimRight(sets, " \t\n\r") + ", `version` = `version` + 1 " + trimmed[where:], true
}

// topLevelWhere returns the index of the WHERE keyword that belongs to this statement.
//
// "Top level" means outside every string literal, backticked identifier and parenthesis — so a
// value like 'where id = 1', a column called `where`, and a nested subquery's WHERE are all
// skipped. The statement is refused outright if it contains a comment, a second statement, or an
// unterminated literal: each of those means the text does not say what it appears to say, and this
// function splices text into it.
func topLevelWhere(q string) (int, bool) {
	var depth int
	var quote byte // 0, '\'', '"' or '`'
	found := -1

	for i := 0; i < len(q); i++ {
		ch := q[i]

		if quote != 0 {
			switch {
			case ch == '\\' && quote != '`':
				i++ // a backslash escape consumes the next byte
			case ch == quote:
				// A doubled quote is an escaped quote and stays inside the literal.
				if i+1 < len(q) && q[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}

		switch {
		case ch == '\'' || ch == '"' || ch == '`':
			quote = ch
		case ch == '(':
			depth++
		case ch == ')':
			depth--
		case ch == ';':
			return 0, false // a second statement
		case ch == '#':
			return 0, false // a comment
		case ch == '-' && i+1 < len(q) && q[i+1] == '-':
			return 0, false
		case ch == '/' && i+1 < len(q) && q[i+1] == '*':
			return 0, false
		case depth == 0 && (ch == 'w' || ch == 'W') && isWhereAt(q, i):
			if found >= 0 {
				return 0, false // two top-level WHEREs is not a shape this understands
			}
			found = i
			i += len("where") - 1
		}
	}

	if quote != 0 {
		return 0, false // unterminated literal
	}
	if found < 0 {
		return 0, false
	}
	return found, true
}

// isWhereAt reports whether the word "where" starts at i and stands alone.
func isWhereAt(q string, i int) bool {
	const kw = "where"
	if i+len(kw) > len(q) || !strings.EqualFold(q[i:i+len(kw)], kw) {
		return false
	}
	if i > 0 && isIdentByte(q[i-1]) {
		return false
	}
	if j := i + len(kw); j < len(q) && isIdentByte(q[j]) {
		return false
	}
	return true
}

func isIdentByte(b byte) bool {
	return b == '_' || b == '$' ||
		(b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

var (
	deletePrefixRe  = regexp.MustCompile(`(?i)^delete\s+from\s`)
	updatePrefixRe  = regexp.MustCompile(`(?i)^update\s`)
	versionAssignRe = regexp.MustCompile("(?i)(^|[\\s,`])`?version`?\\s*=")
)
