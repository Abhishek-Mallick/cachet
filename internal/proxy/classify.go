// Package proxy speaks the MySQL wire protocol so an application can use Cachet without an SDK.
//
// The thesis survives a proxy only because this one carries WRITES as well as reads. "A proxy
// cannot see affected rows" is true of a proxy that intercepts reads alone; one that also carries
// the write is inside the session and can resolve affected rows exactly as the engine does. What a
// proxy genuinely cannot do is hold a session token for the caller, so a connection IS the session
// here — which is the same scope MySQL itself gives you.
package proxy

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
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

// KeyTerm is where one primary key column's value comes from.
//
// Literal and parameter are one type rather than two code paths because a prepared statement and a
// literal one must classify identically. Prepared statements are how most applications talk to
// MySQL — any query given arguments becomes one — so a proxy that understood only literal SQL would
// cache nothing for them and, worse, would forward their WRITES without maintaining the version
// column.
type KeyTerm struct {
	// Literal is the value as written, when IsParam is false.
	Literal string

	IsParam bool

	// Param is the zero-based index of the argument carrying this column's value.
	Param int
}

// Plan is what the proxy decided about one statement.
type Plan struct {
	Kind Kind

	// Key holds one term per primary key column, in the table's declared key order. Set for
	// PointSelect and PointWrite.
	Key []KeyTerm

	// Columns is what a PointSelect asked for, in the order it asked. The proxy must return
	// exactly these, named exactly this way, or it has answered a different question.
	Columns []string

	// BeginsTransaction and EndsTransaction are reported even though the statement is forwarded:
	// inside a transaction a cached read can contradict what the transaction has already written,
	// so the connection stops serving from the cache until it ends.
	BeginsTransaction bool
	EndsTransaction   bool
}

// Matcher classifies statements against one declared table.
//
// Built at boot from a descriptor, because everything it needs to know — the table's name, its
// primary key columns, which columns a cache entry can reconstruct — is a declaration rather than
// something to rediscover per statement. Compiling the patterns once also means the table name is
// part of the pattern instead of a comparison made afterwards, so a statement against a table this
// proxy does not cache never looks like a match in the first place.
type Matcher struct {
	d *schema.Descriptor

	// wholeTable reports that the descriptor declares every column the live table has, which is
	// what makes `SELECT *` answerable. See NewMatcher.
	wholeTable bool

	// pkOrder maps a primary key column's name to its position in the declared key order, so a
	// predicate written in either order resolves to the same key.
	pkOrder map[string]int

	pointSelectRe *regexp.Regexp
	pointWriteRe  *regexp.Regexp
	anyWriteRe    *regexp.Regexp

	// versionAssign notices that a statement already maintains the version column, so the rewrite
	// does not assign it twice.
	versionAssign *regexp.Regexp

	// versionStmt reads a row's version by primary key. Built at boot from validated identifiers,
	// with the key values bound: the only SQL this package sends that it wrote itself.
	versionStmt string
}

// NewMatcher compiles the patterns for one table.
//
// wholeTable is a BOOT FACT, not a guess: the caller establishes it by comparing the descriptor
// against INFORMATION_SCHEMA (see storage.UndeclaredColumns). It is what turns `SELECT *` from
// unconditionally refused into servable — a cache entry holds the declared columns, so `*` can only
// be answered when the declared columns are the whole row. Passing true without having checked
// would make the proxy answer `SELECT *` with a row that silently omits columns.
func NewMatcher(d *schema.Descriptor, wholeTable bool) (*Matcher, error) {
	if d == nil {
		return nil, fmt.Errorf("proxy: no table descriptor")
	}

	m := &Matcher{d: d, wholeTable: wholeTable, pkOrder: make(map[string]int, len(d.PrimaryKey))}
	for i, col := range d.PrimaryKey {
		m.pkOrder[col.Name] = i
	}

	// The table name is embedded rather than compared afterwards. Identifiers reaching here have
	// already passed the schema package's validation, and QuoteMeta is belt and braces: a name that
	// somehow carried a metacharacter would otherwise become part of the grammar.
	name := regexp.QuoteMeta(strings.ToLower(d.Name))
	tbl := "(?:`?[a-z_][a-z0-9_$]*`?\\.)?`?" + name + "`?"

	// The column list may not contain a parenthesis, which is what keeps COUNT(*) and every other
	// function out. `*` is admitted as its own alternative and checked against wholeTable below.
	// The tail after the predicate must be empty or a LIMIT 1, which keeps FOR UPDATE, UNION,
	// ORDER BY and every other clause out.
	m.pointSelectRe = regexp.MustCompile(
		`^select\s+(\*|[a-z0-9_$,` + "`" + `\s]+?)\s+from\s+` + tbl +
			`\s+where\s+(` + conjunction + `)\s*(?:limit\s+1\s*)?$`)

	m.pointWriteRe = regexp.MustCompile(
		`^(?:update\s+` + tbl + `\s+set\s+([^;]*?)|delete\s+from\s+` + tbl + `)` +
			`\s+where\s+(` + conjunction + `)\s*$`)

	m.anyWriteRe = regexp.MustCompile(
		`^(?:update\s+` + tbl +
			`|delete\s+from\s+` + tbl +
			`|insert(?:\s+ignore)?\s+into\s+` + tbl +
			`|replace\s+into\s+` + tbl +
			`|truncate\s+(?:table\s+)?` + tbl + `)\b`)

	m.versionAssign = versionAssignPattern(d.VersionColumn)

	where := make([]string, len(d.PrimaryKey))
	for i, col := range d.PrimaryKey {
		where[i] = col.Quoted() + " = ?"
	}
	m.versionStmt = "SELECT " + d.VersionColumn.Quoted() + " FROM " + d.QuotedName() +
		" WHERE " + strings.Join(where, " AND ")

	return m, nil
}

// Descriptor is the table this matcher classifies against.
func (m *Matcher) Descriptor() *schema.Descriptor { return m.d }

// ServesWholeRow reports whether `SELECT *` is answerable from a cache entry.
func (m *Matcher) ServesWholeRow() bool { return m.wholeTable }

// conjunction is the shape of a WHERE clause this classifier will read.
//
// Deliberately a character class rather than a grammar: identifiers, integers, placeholders, `=`
// and whitespace, and nothing else. Every quote, parenthesis, comma and comparison operator is
// excluded here, before anything is parsed, so the parse below only ever sees text that cannot be
// hiding a subquery, a string literal or an inequality. The terms themselves are then checked one
// by one.
//
// Non-greedy, so a trailing `LIMIT 1` is left for the clause that follows rather than swallowed
// into the predicate — the character class admits letters, and "id = 42 limit 1" would otherwise
// be read as one term and refused.
const conjunction = "[a-z0-9_$`\\s=?]+?"

var termRe = regexp.MustCompile("^`?([a-z_][a-z0-9_$]*)`?\\s*=\\s*([0-9]+|\\?)$")

// Classify decides what may be done with a statement. It claims nothing it cannot prove.
//
// This is deliberately a matcher over a normalised statement rather than a SQL parser. A parser
// would recognise more, and every additional shape it recognised would be a new way to be wrong
// about what a statement means — which, here, means serving a row the caller did not ask for. The
// cost of refusing to understand a statement is a cache miss. The cost of misunderstanding one is a
// correctness bug, and those are not comparable.
func (m *Matcher) Classify(query string) Plan {
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

	if sub := m.pointSelectRe.FindStringSubmatch(q); sub != nil {
		cols, ok := m.selectedColumns(sub[1])
		if !ok {
			return Plan{}
		}
		key, ok := m.keyFrom(q, sub[2])
		if !ok {
			return Plan{}
		}
		return Plan{Kind: PointSelect, Key: key, Columns: cols}
	}

	if sub := m.pointWriteRe.FindStringSubmatch(q); sub != nil {
		if key, ok := m.keyFrom(q, sub[2]); ok {
			return Plan{Kind: PointWrite, Key: key}
		}
	}

	// Any remaining statement that writes the cached table is one we could not pin down.
	if m.anyWriteRe.MatchString(q) {
		return Plan{Kind: OpaqueWrite}
	}
	return Plan{}
}

// selectedColumns resolves a select list against the descriptor.
//
// A cache entry holds the declared columns and nothing else, so a list naming an undeclared column
// — `updated_at`, or anything the deployment chose not to cache — cannot be answered without
// inventing a value. Those are refused rather than served as a row that looks right.
func (m *Matcher) selectedColumns(list string) ([]string, bool) {
	if strings.TrimSpace(list) == "*" {
		if !m.wholeTable {
			return nil, false
		}
		out := make([]string, len(m.d.Columns))
		for i := range m.d.Columns {
			out[i] = m.d.Columns[i].Name
		}
		return out, true
	}

	parts := strings.Split(list, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		col := strings.Trim(strings.TrimSpace(p), "`")
		if col == "" || m.d.Column(col) == nil {
			return nil, false
		}
		out = append(out, col)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// keyFrom reads a WHERE clause as a complete primary key.
//
// The clause must name every primary key column exactly once and nothing else. Written in either
// order: a predicate is a set of equalities, and requiring the declared order would refuse
// statements that mean precisely the same thing. An extra term is a refusal rather than an
// intersection — `WHERE id = 1 AND tenant_id = 2` selects a subset of what `WHERE id = 1` selects,
// and serving the cached row for id 1 would answer a question the caller did not ask.
func (m *Matcher) keyFrom(q, clause string) ([]KeyTerm, bool) {
	terms := andRe.Split(clause, -1)
	if len(terms) != len(m.d.PrimaryKey) {
		return nil, false
	}

	key := make([]KeyTerm, len(m.d.PrimaryKey))
	seen := make([]bool, len(m.d.PrimaryKey))
	params := 0

	for _, raw := range terms {
		sub := termRe.FindStringSubmatch(strings.TrimSpace(raw))
		if sub == nil {
			return nil, false
		}
		pos, ok := m.pkOrder[sub[1]]
		if !ok || seen[pos] {
			return nil, false
		}
		seen[pos] = true

		if sub[2] == "?" {
			key[pos] = KeyTerm{IsParam: true}
			params++
			continue
		}
		key[pos] = KeyTerm{Literal: sub[2]}
	}

	if params == 0 {
		return key, true
	}

	// Placeholder positions are counted over the whole statement, because that is how the client
	// numbers its arguments. Every placeholder must be accounted for: one this classifier has not
	// placed means it does not know what the statement will do once bound, and it refuses rather
	// than guess.
	if err := m.placePlaceholders(q, clause, key); err != nil {
		return nil, false
	}
	return key, true
}

// placePlaceholders assigns each parameterised key column its argument index.
func (m *Matcher) placePlaceholders(q, clause string, key []KeyTerm) error {
	start := strings.LastIndex(q, clause)
	if start < 0 {
		return fmt.Errorf("proxy: the predicate is not where it was found")
	}
	if strings.Count(q[start+len(clause):], "?") != 0 {
		return fmt.Errorf("proxy: a placeholder follows the predicate")
	}

	// The index of the first placeholder inside the predicate, in the statement's own numbering.
	base := strings.Count(q[:start], "?")

	// Within the predicate, placeholders are numbered in the order they are WRITTEN, which is the
	// order the terms were split in — not the declared key order.
	nth := 0
	for _, raw := range andRe.Split(clause, -1) {
		sub := termRe.FindStringSubmatch(strings.TrimSpace(raw))
		if sub == nil {
			return fmt.Errorf("proxy: the predicate no longer parses")
		}
		if sub[2] != "?" {
			continue
		}
		pos := m.pkOrder[sub[1]]
		key[pos].Param = base + nth
		nth++
	}
	return nil
}

// Key resolves a plan's primary key for one execution, from literals and bound arguments.
//
// It reports false for anything it cannot turn into a key with certainty — a missing argument, a
// value that does not fit its column's type, a NULL. The caller then forwards the statement, which
// is always safe.
func (m *Matcher) Key(p Plan, args []any) (schema.Key, bool) {
	if len(p.Key) != len(m.d.PrimaryKey) {
		return schema.Key{}, false
	}

	values := make([]any, len(p.Key))
	for i, term := range p.Key {
		var text string
		if term.IsParam {
			if term.Param < 0 || term.Param >= len(args) {
				return schema.Key{}, false
			}
			s, ok := argText(args[term.Param])
			if !ok {
				return schema.Key{}, false
			}
			text = s
		} else {
			text = term.Literal
		}
		if !fitsColumn(m.d.PrimaryKey[i], text) {
			return schema.Key{}, false
		}
		values[i] = text
	}

	k, err := m.d.Key(values...)
	if err != nil {
		return schema.Key{}, false
	}
	return k, true
}

// argText renders a bound argument as the canonical text its column would hold.
//
// A float is refused rather than formatted: the rendering of a float is a choice, and a key built
// from one choice would not match a key built from another.
func argText(v any) (string, bool) {
	switch a := v.(type) {
	case uint64:
		return strconv.FormatUint(a, 10), true
	case int64:
		return strconv.FormatInt(a, 10), true
	case int:
		return strconv.Itoa(a), true
	case string:
		return a, true
	case []byte:
		return string(a), true
	default:
		return "", false
	}
}

// fitsColumn checks a key value against the column that will hold it.
//
// An out-of-range integer is not a row this proxy will claim to know about: MySQL would reject or
// truncate it, and either way the row the caller meant is not the row the key names.
func fitsColumn(col *schema.Column, text string) bool {
	switch col.Type {
	case schema.Uint8:
		v, err := strconv.ParseUint(text, 10, 8)
		return err == nil && text == strconv.FormatUint(v, 10)
	case schema.Uint32:
		v, err := strconv.ParseUint(text, 10, 32)
		return err == nil && text == strconv.FormatUint(v, 10)
	case schema.Uint64:
		v, err := strconv.ParseUint(text, 10, 64)
		return err == nil && text == strconv.FormatUint(v, 10)
	case schema.Int64:
		v, err := strconv.ParseInt(text, 10, 64)
		return err == nil && text == strconv.FormatInt(v, 10)
	case schema.String, schema.Bytes, schema.Text:
		return true
	default:
		return false
	}
}

var (
	beginRe = regexp.MustCompile(`^(?:begin|start transaction)$`)
	endRe   = regexp.MustCompile(`^(?:commit|rollback)$`)
	andRe   = regexp.MustCompile(`\s+and\s+`)
)

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
