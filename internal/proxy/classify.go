// Package proxy speaks the MySQL wire protocol so an application can use Cachet without an SDK.
//
// The thesis survives a proxy only because this one carries WRITES as well as reads. "A proxy
// cannot see affected rows" is true of a proxy that intercepts reads alone; one that also carries
// the write is inside the session and can resolve affected rows exactly as the engine does. What a
// proxy genuinely cannot do is hold a session token for the caller, so a connection IS the session
// here — which is the same scope MySQL itself gives you.
package proxy

import (
	"regexp"
	"strconv"
	"strings"
)

// Kind is what the proxy may do with a statement.
type Kind int

const (
	// Passthrough sends the statement upstream untouched and caches nothing. Always safe.
	Passthrough Kind = iota

	// PointSelect is a read of exactly one row of the cached table, by primary key.
	PointSelect

	// PointWrite changes exactly one row of the cached table, named by primary key.
	PointWrite

	// OpaqueWrite changes the cached table in a way this classifier cannot pin to specific rows.
	//
	// Distinct from Passthrough because the consequence is different: the statement is forwarded
	// either way, but an OpaqueWrite leaves cache entries that no invalidation will reach on the
	// write path. It falls to the CDC backstop, which means those rows drop from SESSION to
	// BOUNDED(cdc_lag) — a real weakening, and the caller is told rather than left to find out.
	OpaqueWrite
)

func (k Kind) String() string {
	switch k {
	case PointSelect:
		return "PointSelect"
	case PointWrite:
		return "PointWrite"
	case OpaqueWrite:
		return "OpaqueWrite"
	default:
		return "Passthrough"
	}
}

// Plan is what the proxy decided about one statement.
type Plan struct {
	Kind Kind

	// ID is the primary key, set for PointSelect and PointWrite.
	ID uint64

	// Columns is what a PointSelect asked for, in the order it asked. The proxy must return
	// exactly these, named exactly this way, or it has answered a different question.
	Columns []string

	// IDIsParam reports that the id arrives as a bound argument rather than a literal, and IDParam
	// is which one. Prepared statements are how most applications talk to MySQL — any query given
	// arguments becomes one — so a proxy that only understood literal SQL would cache nothing for
	// them and, worse, would forward their WRITES without maintaining the version column.
	IDIsParam bool
	IDParam   int

	// BeginsTransaction and EndsTransaction are reported even though the statement is forwarded:
	// inside a transaction a cached read can contradict what the transaction has already written,
	// so the connection stops serving from the cache until it ends.
	BeginsTransaction bool
	EndsTransaction   bool
}

// Classify decides what may be done with a statement. It claims nothing it cannot prove.
//
// This is deliberately a matcher over a normalised statement rather than a SQL parser. A parser
// would recognise more, and every additional shape it recognised would be a new way to be wrong
// about what a statement means — which, here, means serving a row the caller did not ask for. The
// cost of refusing to understand a statement is a cache miss. The cost of misunderstanding one is a
// correctness bug, and those are not comparable.
func Classify(cachedTable, query string) Plan {
	q, ok := normalise(query)
	if !ok {
		return Plan{}
	}

	switch {
	case beginRe.MatchString(q):
		return Plan{BeginsTransaction: true}
	case endRe.MatchString(q):
		return Plan{EndsTransaction: true}
	}

	if m := pointSelectRe.FindStringSubmatch(q); m != nil {
		cols, ok := cacheableColumns(m[1])
		if !ok || !tableMatches(cachedTable, m[2]) {
			return Plan{}
		}
		p, ok := idTarget(q, m[3])
		if !ok {
			return Plan{}
		}
		p.Kind, p.Columns = PointSelect, cols
		return p
	}

	if m := pointWriteRe.FindStringSubmatch(q); m != nil {
		// m[1] is the UPDATE table, m[3] the DELETE table; exactly one is non-empty.
		if name := m[1] + m[3]; tableMatches(cachedTable, name) {
			if p, ok := idTarget(q, m[4]); ok {
				p.Kind = PointWrite
				return p
			}
		}
	}

	// Any remaining statement that writes the cached table is one we could not pin down.
	if m := anyWriteRe.FindStringSubmatch(q); m != nil {
		for _, name := range m[1:] {
			if name != "" && tableMatches(cachedTable, name) {
				return Plan{Kind: OpaqueWrite}
			}
		}
	}
	return Plan{}
}

// The identifier fragment: an optional database qualifier, optional backticks, one name.
const ident = "(?:`?[a-z_][a-z0-9_$]*`?\\.)?`?([a-z_][a-z0-9_$]*)`?"

var (
	beginRe = regexp.MustCompile(`^(?:begin|start transaction)$`)
	endRe   = regexp.MustCompile(`^(?:commit|rollback)$`)

	// A point select, and nothing else. The column list may not contain a parenthesis, which is
	// what keeps COUNT(*) and every other function out; the tail after the id must be empty or a
	// LIMIT, which keeps FOR UPDATE, UNION, ORDER BY and extra predicates out.
	pointSelectRe = regexp.MustCompile(
		`^select\s+([a-z0-9_$,` + "`" + `\s]+?)\s+from\s+` + ident +
			`\s+where\s+` + "`?id`?" + `\s*=\s*([0-9]+|\?)\s*(?:limit\s+1\s*)?$`)

	// Either UPDATE <t> SET ... WHERE id = n, or DELETE FROM <t> WHERE id = n.
	pointWriteRe = regexp.MustCompile(
		`^(?:update\s+` + ident + `\s+set\s+([^;]*?)|delete\s+from\s+` + ident + `)` +
			`\s+where\s+` + "`?id`?" + `\s*=\s*([0-9]+|\?)\s*$`)

	// Anything that modifies a table, used only to notice writes to the cached one.
	anyWriteRe = regexp.MustCompile(
		`^(?:update\s+` + ident +
			`|delete\s+from\s+` + ident +
			`|insert(?:\s+ignore)?\s+into\s+` + ident +
			`|replace\s+into\s+` + ident +
			`|truncate\s+(?:table\s+)?` + ident + `)\b`)
)

// boundID resolves the row id for one execution, from the literal or from the arguments.
//
// It reports false for anything it cannot turn into a row id with certainty — a missing argument,
// a negative number, a string that is not a number. The caller then forwards the statement, which
// is always safe.
func (p Plan) boundID(args []any) (uint64, bool) {
	if !p.IDIsParam {
		return p.ID, p.ID != 0
	}
	if p.IDParam < 0 || p.IDParam >= len(args) {
		return 0, false
	}
	switch v := args[p.IDParam].(type) {
	case uint64:
		return v, v != 0
	case int64:
		if v <= 0 {
			return 0, false
		}
		return uint64(v), true
	case int:
		if v <= 0 {
			return 0, false
		}
		return uint64(v), true
	case string:
		id, err := strconv.ParseUint(v, 10, 64)
		return id, err == nil && id != 0
	case []byte:
		id, err := strconv.ParseUint(string(v), 10, 64)
		return id, err == nil && id != 0
	default:
		return 0, false
	}
}

// idTarget resolves where the row id comes from: a literal, or a bound argument.
//
// When it is a placeholder, the argument index is the number of placeholders before it. Every
// placeholder in the statement must be accounted for — one the classifier has not placed means it
// does not know what the statement will do once bound, and it refuses rather than guess.
func idTarget(q, token string) (Plan, bool) {
	if token != "?" {
		id, ok := parseID(token)
		return Plan{ID: id}, ok
	}

	idx := strings.LastIndex(q, "?")
	if idx < 0 {
		return Plan{}, false
	}
	// The id placeholder must be the LAST one: both recognised shapes end with `WHERE id = ?`, so
	// anything after it is a shape this does not understand.
	if strings.Count(q[idx+1:], "?") != 0 {
		return Plan{}, false
	}
	return Plan{IDIsParam: true, IDParam: strings.Count(q[:idx], "?")}, true
}

// cacheable is what a cache entry actually holds.
//
// Deliberately not the table's full column list. `updated_at` exists in the schema and NOT in the
// entry, so a `SELECT *` — or any list naming it — cannot be answered from the cache without
// inventing a value. The proxy refuses those rather than returning a row that looks right.
var cacheable = map[string]bool{
	"id": true, "tenant_id": true, "status": true, "payload": true, "version": true,
}

// cacheableColumns splits a select list and returns it only if every column can be reconstructed.
func cacheableColumns(list string) ([]string, bool) {
	parts := strings.Split(list, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		col := strings.Trim(strings.TrimSpace(p), "`")
		if col == "" || !cacheable[col] {
			return nil, false
		}
		out = append(out, col)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// tableMatches compares an unqualified table name.
func tableMatches(cached, found string) bool {
	return strings.EqualFold(strings.Trim(cached, "`"), strings.Trim(found, "`"))
}

// parseID refuses anything that is not a plain unsigned integer in range. A row id that does not
// parse is not a row this proxy will claim to know about.
func parseID(s string) (uint64, bool) {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil || id == 0 {
		return 0, false
	}
	return id, true
}

// normalise lowercases, strips comments, and collapses whitespace.
//
// It returns false for anything it will not reason about: an empty statement, or one containing
// more than one statement. Comments are removed rather than tolerated because a matcher that
// ignored them could be shown a predicate it never saw — `WHERE id = 1 /* AND tenant = 2 */` and
// `WHERE id = 1 -- AND tenant = 2` mean different things to MySQL than to a naive regex.
func normalise(query string) (string, bool) {
	q := strings.ToLower(query)

	// Two kinds of /* */ are not comments at all and must not be stripped:
	//
	//	/*! ... */   a version-gated block, which MySQL EXECUTES
	//	/*+ ... */   an optimizer hint, which asks for execution this proxy is about to skip
	//
	// Removing either changes what the statement means or what the caller asked for, so a
	// statement containing one is refused rather than understood.
	if strings.Contains(q, "/*!") || strings.Contains(q, "/*+") {
		return "", false
	}
	// A block comment is removed only if it is closed; an unterminated one means the statement is
	// not what it appears to be, and it is refused.
	if strings.Contains(q, "/*") {
		if !strings.Contains(q, "*/") {
			return "", false
		}
		q = blockCommentRe.ReplaceAllString(q, " ")
	}
	// "--" begins a comment in MySQL only when followed by whitespace or end of line. "--5" is two
	// unary minuses, so truncating there would silently change the statement.
	if i := strings.Index(q, "--"); i >= 0 {
		rest := q[i+2:]
		if rest != "" && !isSpace(rest[0]) {
			return "", false
		}
		q = q[:i]
	}
	if i := strings.Index(q, "#"); i >= 0 {
		q = q[:i]
	}

	q = strings.TrimSpace(q)
	q = strings.TrimSuffix(q, ";")

	// More than one statement: refuse. Splitting them would mean reasoning about each, and a
	// proxy that rewrites multi-statement traffic is a proxy with a new class of bug.
	if strings.Contains(q, ";") {
		return "", false
	}

	q = strings.TrimSpace(whitespaceRe.ReplaceAllString(q, " "))
	if q == "" {
		return "", false
	}
	return q, true
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

var (
	blockCommentRe = regexp.MustCompile(`/\*.*?\*/`)
	whitespaceRe   = regexp.MustCompile(`\s+`)
)
