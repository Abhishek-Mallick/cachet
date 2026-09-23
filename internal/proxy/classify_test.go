package proxy_test

import (
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/proxy"
	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// entitiesMatcher is the fixture table, declared the way a deployment declares its own.
//
// wholeTable is false because the live `entities` has an `updated_at` column the descriptor does
// not declare — which is exactly the condition that makes `SELECT *` unanswerable from a cache
// entry. See TestSelectStarIsServedOnlyWhenTheDeclarationIsTheWholeRow for the other branch.
func entitiesMatcher(t *testing.T) *proxy.Matcher {
	t.Helper()
	return matcherFor(t, false, schema.TableConfig{
		Name:          "entities",
		PrimaryKey:    []string{"id"},
		VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "id", Type: schema.Uint64},
			{Name: "tenant_id", Type: schema.Uint32},
			{Name: "status", Type: schema.Uint8},
			{Name: "payload", Type: schema.Bytes},
			{Name: "version", Type: schema.Uint64},
		},
	})
}

func matcherFor(t *testing.T, wholeTable bool, cfg schema.TableConfig) *proxy.Matcher {
	t.Helper()
	d, err := schema.NewDescriptor(cfg)
	if err != nil {
		t.Fatalf("NewDescriptor: %v", err)
	}
	m, err := proxy.NewMatcher(d, wholeTable)
	if err != nil {
		t.Fatalf("NewMatcher: %v", err)
	}
	return m
}

// keyOf resolves a plan's key the way the server does, so a test asserting which row a statement
// names is asserting the same thing the proxy will act on.
func keyOf(t *testing.T, m *proxy.Matcher, p proxy.Plan, args ...any) string {
	t.Helper()
	k, ok := m.Key(p, args)
	if !ok {
		t.Fatalf("the plan named no row this matcher could resolve")
	}
	return k.String()
}

// The classifier's contract is asymmetric, and that asymmetry is the whole design.
//
// Classifying a statement as PASSTHROUGH is always safe: the query goes to the database and the
// only cost is a cache miss. Classifying one as cacheable when it is not is a correctness bug — it
// serves a row from the cache that the statement did not ask for, or misses an invalidation.
//
// So the rule is: claim nothing that is not proven. Every test below that expects Passthrough is
// a statement the classifier must REFUSE to be clever about.

func TestAPointSelectIsRecognised(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, q := range []string{
		"SELECT id, tenant_id, status, payload, version FROM entities WHERE id = 42",
		"select payload from entities where id = 42",
		"SELECT status, payload FROM entities WHERE id = 42;",
		"  SELECT   payload   FROM   entities   WHERE   id   =   42  ",
		"SELECT payload FROM entities WHERE id = 42 LIMIT 1",
		"SELECT `payload` FROM `entities` WHERE `id` = 42",
		"SELECT payload FROM cachet.entities WHERE id = 42",
		// A real comment, which MySQL discards and so must the proxy: this IS a point select.
		"SELECT payload FROM entities WHERE id = 42 -- AND tenant_id = 9",
		"SELECT payload FROM entities WHERE id = 42 # AND tenant_id = 9",
	} {
		p := m.Classify(q)
		if p.Kind != proxy.PointSelect {
			t.Errorf("Classify(%q) = %v, want PointSelect", q, p.Kind)
			continue
		}
		if got := keyOf(t, m, p); got != "entities:42" {
			t.Errorf("Classify(%q) named %q, want \"entities:42\"", q, got)
		}
		if len(p.Columns) == 0 {
			t.Errorf("Classify(%q) recognised a point select but named no columns; the proxy "+
				"cannot build a result set it cannot describe", q)
		}
	}
}

// The column list the caller asked for is the column list it must get back, in that order.
func TestTheRequestedColumnsAreReportedInOrder(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	p := m.Classify("SELECT status, id, payload FROM entities WHERE id = 42")
	if p.Kind != proxy.PointSelect {
		t.Fatalf("not recognised: %v", p.Kind)
	}
	want := []string{"status", "id", "payload"}
	if len(p.Columns) != len(want) {
		t.Fatalf("columns = %v, want %v", p.Columns, want)
	}
	for i := range want {
		if p.Columns[i] != want[i] {
			t.Errorf("column %d = %q, want %q", i, p.Columns[i], want[i])
		}
	}
}

// Everything the classifier must refuse. Each of these could be served wrongly by a classifier that
// pattern-matched loosely, and each one is a real query somebody writes.
func TestAnythingUnprovenIsPassedThrough(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, q := range []string{
		// SELECT * cannot be served from the cache: the entry holds id, tenant_id, status, payload
		// and version, and NOT updated_at. Returning a row without it, or with a value invented for
		// it, would be answering a different question than the one asked.
		"SELECT * FROM entities WHERE id = 42",
		"select * from entities where id = 42",
		"SELECT updated_at FROM entities WHERE id = 42",
		"SELECT payload, updated_at FROM entities WHERE id = 42",
		"SELECT nonexistent FROM entities WHERE id = 42",
		// Not a single row.
		"SELECT * FROM entities WHERE id > 42",
		"SELECT * FROM entities WHERE id IN (42, 43)",
		"SELECT * FROM entities WHERE id = 42 OR id = 43",
		"SELECT * FROM entities WHERE tenant_id = 42",
		"SELECT * FROM entities",
		// Not a plain row read: the cache holds rows, not computed results.
		"SELECT COUNT(*) FROM entities WHERE id = 42",
		"SELECT MAX(version) FROM entities WHERE id = 42",
		// Another table, or more than one.
		"SELECT * FROM orders WHERE id = 42",
		"SELECT * FROM entities JOIN orders ON orders.id = entities.id WHERE entities.id = 42",
		"SELECT * FROM entities, orders WHERE entities.id = 42",
		// Reads that must see the database.
		"SELECT * FROM entities WHERE id = 42 FOR UPDATE",
		"SELECT * FROM entities WHERE id = 42 LOCK IN SHARE MODE",
		"SELECT * FROM entities WHERE id = 42 FOR SHARE",
		// Subqueries and set operations.
		"SELECT * FROM (SELECT * FROM entities WHERE id = 42) x",
		"SELECT * FROM entities WHERE id = 42 UNION SELECT * FROM entities WHERE id = 43",
		"SELECT * FROM entities WHERE id = (SELECT MAX(id) FROM entities)",
		// More than one statement in one string.
		"SELECT * FROM entities WHERE id = 42; DROP TABLE entities",
		// A comment cannot hide a clause, but an executable or hint block is not a comment.
		"SELECT * FROM entities WHERE id = 42 /* trailing */ AND tenant_id = 9",
		"SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM entities WHERE id = 42",
		"SELECT /*!40001 SQL_NO_CACHE */ * FROM entities WHERE id = 42",
		"SELECT * FROM entities WHERE id = 42 /* unterminated",
		// "--" is only a comment when whitespace follows it; "--5" is two unary minuses.
		"SELECT * FROM entities WHERE id = --5",
		// Not a read at all.
		"SHOW TABLES",
		"SET autocommit = 0",
		"BEGIN",
		"", "   ",
		// A non-numeric or absurd id.
		"SELECT * FROM entities WHERE id = 'abc'",
		"SELECT * FROM entities WHERE id = 42.5",
		"SELECT * FROM entities WHERE id = -1",
		"SELECT * FROM entities WHERE id = 99999999999999999999999",
	} {
		if p := m.Classify(q); p.Kind != proxy.Passthrough {
			t.Errorf("Classify(%q) = %v, want Passthrough — the classifier was clever where it "+
				"had no proof", q, p.Kind)
		}
	}
}

func TestAPointWriteNamesTheRowItTouches(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, tc := range []struct {
		q   string
		key string
	}{
		{"UPDATE entities SET status = 1 WHERE id = 7", "entities:7"},
		{"update entities set status = 1, payload = 'x' where id = 7", "entities:7"},
		{"DELETE FROM entities WHERE id = 7", "entities:7"},
		{"delete from `entities` where `id` = 7;", "entities:7"},
	} {
		p := m.Classify(tc.q)
		if p.Kind != proxy.PointWrite {
			t.Errorf("Classify(%q) = %v, want PointWrite", tc.q, p.Kind)
			continue
		}
		if got := keyOf(t, m, p); got != tc.key {
			t.Errorf("Classify(%q) named %q, want %q", tc.q, got, tc.key)
		}
	}
}

// A write to the cached table that the classifier cannot pin to specific rows is the dangerous
// case: passing it through silently would leave stale entries behind. It must be reported so the
// caller can decide, not quietly treated as harmless.
func TestAnUnrecognisedWriteToTheCachedTableIsReported(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, q := range []string{
		"UPDATE entities SET status = 1 WHERE tenant_id = 9",
		"UPDATE entities SET status = 1",
		"DELETE FROM entities WHERE tenant_id = 9",
		"DELETE FROM entities",
		"INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (1, 2, 3, 'x', 4)",
		"REPLACE INTO entities (id) VALUES (1)",
		"TRUNCATE TABLE entities",
	} {
		if p := m.Classify(q); p.Kind != proxy.OpaqueWrite {
			t.Errorf("Classify(%q) = %v, want OpaqueWrite — an unpinnable write to the cached "+
				"table cannot be treated as harmless", q, p.Kind)
		}
	}
}

// A write to a table nobody caches is genuinely none of the proxy's business.
func TestAWriteToAnotherTableIsPassedThrough(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, q := range []string{
		"UPDATE orders SET status = 1 WHERE id = 7",
		"DELETE FROM audit_log WHERE id = 7",
		"INSERT INTO orders (id) VALUES (1)",
	} {
		if p := m.Classify(q); p.Kind != proxy.Passthrough {
			t.Errorf("Classify(%q) = %v, want Passthrough", q, p.Kind)
		}
	}
}

// Transaction control has to be visible to the proxy even though it forwards it: inside a
// transaction, a cached read can contradict what the transaction has already written.
func TestTransactionControlIsRecognised(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, q := range []string{"BEGIN", "begin", "START TRANSACTION", "start transaction  ;"} {
		if p := m.Classify(q); p.Kind != proxy.Passthrough || !p.BeginsTransaction {
			t.Errorf("Classify(%q) did not report the start of a transaction", q)
		}
	}
	for _, q := range []string{"COMMIT", "commit;", "ROLLBACK", "rollback  "} {
		if p := m.Classify(q); p.Kind != proxy.Passthrough || !p.EndsTransaction {
			t.Errorf("Classify(%q) did not report the end of a transaction", q)
		}
	}
}

// Rewriting a statement is the most dangerous thing this package does: the result is executed
// against the caller's database. It must change exactly one thing — the version column — and
// refuse anything it cannot change safely.
func TestTheVersionBumpIsAddedToASingleRowUpdate(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	got, ok := proxy.RewriteForTest(m, "UPDATE entities SET status = 1 WHERE id = 7")
	if !ok {
		t.Fatal("refused a statement it must be able to rewrite")
	}
	if got != "UPDATE entities SET status = 1, `version` = `version` + 1 WHERE id = 7" { //nolint:goconst // the expected SQL reads better inline
		t.Errorf("rewrote to:\n  %s", got)
	}
}

func TestADeleteNeedsNoRewrite(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	const q = "DELETE FROM entities WHERE id = 7"
	got, ok := proxy.RewriteForTest(m, q)
	if !ok || got != q {
		t.Errorf("a delete removes the row, so there is no version left to bump; got %q, ok=%v", got, ok)
	}
}

// A statement that already sets the version is the application maintaining it itself. Adding a
// second assignment would be ambiguous at best and wrong at worst, so it is left alone.
func TestAStatementThatAlreadySetsVersionIsLeftAlone(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	const q = "UPDATE entities SET status = 1, version = 99 WHERE id = 7"
	got, ok := proxy.RewriteForTest(m, q)
	if !ok || got != q {
		t.Errorf("got %q, ok=%v — a statement that maintains version itself must pass through unchanged", got, ok)
	}
}

// Anything that could hide a second statement or a string literal containing SQL is refused
// outright. The rewrite appends text to a statement; appending to something misunderstood is how a
// proxy turns a cache into an incident.
func TestTheRewriteRefusesWhatItCannotSeeThrough(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	// String literals and quoted identifiers are resolved by the scanner rather than refused —
	// see TestTheRewriteHandlesStringLiterals. What stays refused is text that does not say what it
	// appears to say.
	for _, q := range []string{
		"UPDATE entities SET status = 1 WHERE id = 7; DROP TABLE entities",
		"UPDATE entities SET status = 1 /* where */ WHERE id = 7",
	} {
		if _, ok := proxy.RewriteForTest(m, q); ok {
			t.Errorf("rewrote a statement it should have refused: %q", q)
		}
	}
}

// Setting a string value is the most ordinary update there is, and the rewrite has to handle it —
// including when the string itself contains SQL keywords, quotes, or escapes.
func TestTheRewriteHandlesStringLiterals(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, tc := range []struct{ in, want string }{
		{
			"UPDATE entities SET payload = 'v2' WHERE id = 7",
			"UPDATE entities SET payload = 'v2', `version` = `version` + 1 WHERE id = 7",
		},
		{
			// The literal contains the word WHERE. Splicing at the wrong one corrupts the statement.
			"UPDATE entities SET payload = 'where id = 1' WHERE id = 7",
			"UPDATE entities SET payload = 'where id = 1', `version` = `version` + 1 WHERE id = 7",
		},
		{
			// A doubled quote inside a literal.
			"UPDATE entities SET payload = 'it''s here WHERE' WHERE id = 7",
			"UPDATE entities SET payload = 'it''s here WHERE', `version` = `version` + 1 WHERE id = 7",
		},
		{
			// A backslash-escaped quote.
			`UPDATE entities SET payload = 'a\' WHERE b' WHERE id = 7`,
			"UPDATE entities SET payload = 'a\\' WHERE b', `version` = `version` + 1 WHERE id = 7",
		},
		{
			// A backticked identifier containing the keyword.
			"UPDATE entities SET `where` = 1 WHERE id = 7",
			"UPDATE entities SET `where` = 1, `version` = `version` + 1 WHERE id = 7",
		},
	} {
		got, ok := proxy.RewriteForTest(m, tc.in)
		if !ok {
			t.Errorf("refused a statement it must handle: %q", tc.in)
			continue
		}
		if got != tc.want {
			t.Errorf("rewrote\n  %q\nto\n  %q\nwant\n  %q", tc.in, got, tc.want)
		}
	}
}

// And it must still refuse what it genuinely cannot see through.
func TestTheRewriteStillRefusesTheDangerousShapes(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, q := range []string{
		"UPDATE entities SET status = 1 WHERE id = 7; DROP TABLE entities",
		"UPDATE entities SET status = 1 /* where */ WHERE id = 7",
		"UPDATE entities SET status = 1 -- x\n WHERE id = 7",
		"UPDATE entities SET payload = 'unterminated WHERE id = 7",
		"UPDATE entities SET status = 1",                         // no predicate at all
		"UPDATE entities SET status = (SELECT 1 FROM t WHERE x)", // only a nested where
	} {
		if got, ok := proxy.RewriteForTest(m, q); ok {
			t.Errorf("rewrote a statement it should have refused:\n  %q\n  → %q", q, got)
		}
	}
}

// Prepared statements carry their values as arguments, so the classifier has to recognise the
// placeholder and say WHICH argument supplies the id.
func TestAPlaceholderIdIsRecognised(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, tc := range []struct {
		q     string
		kind  proxy.Kind
		param int
	}{
		{"SELECT payload FROM entities WHERE id = ?", proxy.PointSelect, 0},
		{"select status, payload from entities where id = ?", proxy.PointSelect, 0},
		{"UPDATE entities SET payload = ? WHERE id = ?", proxy.PointWrite, 1},
		{"UPDATE entities SET status = ?, payload = ? WHERE id = ?", proxy.PointWrite, 2},
		{"DELETE FROM entities WHERE id = ?", proxy.PointWrite, 0},
	} {
		p := m.Classify(tc.q)
		if p.Kind != tc.kind {
			t.Errorf("Classify(%q) = %v, want %v", tc.q, p.Kind, tc.kind)
			continue
		}
		if !p.Key[0].IsParam {
			t.Errorf("Classify(%q) did not report the id as a parameter", tc.q)
			continue
		}
		if p.Key[0].Param != tc.param {
			t.Errorf("Classify(%q) says argument %d supplies the id, want %d", tc.q, p.Key[0].Param, tc.param)
		}
	}
}

// A placeholder anywhere the classifier cannot account for means it does not know what the
// statement will do when executed, so it refuses to claim anything about it.
func TestAPlaceholderItCannotAccountForIsPassedThrough(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	for _, q := range []string{
		"SELECT payload FROM entities WHERE id = ? AND tenant_id = ?",
		"SELECT payload FROM entities WHERE tenant_id = ?",
		"UPDATE entities SET status = 1 WHERE tenant_id = ?",
		"SELECT payload FROM entities WHERE id = ? LIMIT ?",
	} {
		p := m.Classify(q)
		if p.Kind == proxy.PointSelect || p.Kind == proxy.PointWrite {
			t.Errorf("Classify(%q) = %v, want Passthrough or OpaqueWrite", q, p.Kind)
		}
	}
}

// A literal id must not be reported as a parameter, or the handler would look for an argument that
// does not exist.
func TestALiteralIdIsNotAParameter(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)

	if p := m.Classify("SELECT payload FROM entities WHERE id = 42"); p.Key[0].IsParam {
		t.Error("a literal id was reported as a parameter")
	}
}

// ─── tables that are not the fixture ────────────────────────────────────────────

// ordersMatcher is a table with a composite primary key, a string key column, a nullable column
// and a version column that is not called "version" — none of which the classifier used to be
// able to express, because every one of them was spelled into its patterns.
func ordersMatcher(t *testing.T, wholeTable bool) *proxy.Matcher {
	t.Helper()
	return matcherFor(t, wholeTable, schema.TableConfig{
		Name:          "orders",
		PrimaryKey:    []string{"region", "order_no"},
		VersionColumn: "row_version",
		Columns: []schema.ColumnConfig{
			{Name: "region", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "order_no", Type: schema.Uint64},
			{Name: "note", Type: schema.Text, Nullable: true},
			{Name: "row_version", Type: schema.Uint64},
		},
	})
}

// TestSelectStarIsServedOnlyWhenTheDeclarationIsTheWholeRow is the free win that boot verification
// buys, and the reason it must be a proof rather than a setting.
//
// A cache entry holds the declared columns. `*` means every column the table has. Those are the
// same list only when the declaration covers the whole table, and the caller establishes that
// against INFORMATION_SCHEMA before the proxy accepts a connection. Serving `*` without it would
// return a row silently missing a column, which reads as correct until someone uses the column.
func TestSelectStarIsServedOnlyWhenTheDeclarationIsTheWholeRow(t *testing.T) {
	t.Parallel()

	const q = "SELECT * FROM orders WHERE region = ? AND order_no = ?"

	partial := ordersMatcher(t, false)
	if p := partial.Classify(q); p.Kind != proxy.Passthrough {
		t.Errorf("SELECT * was classified as %v against a table with undeclared columns", p.Kind)
	}

	whole := ordersMatcher(t, true)
	p := whole.Classify(q)
	if p.Kind != proxy.PointSelect {
		t.Fatalf("SELECT * = %v, want PointSelect when the declaration is the whole row", p.Kind)
	}
	// Declared order, because that is the order MySQL would have returned `*` in.
	want := []string{"region", "order_no", "note", "row_version"}
	if len(p.Columns) != len(want) {
		t.Fatalf("columns = %v, want %v", p.Columns, want)
	}
	for i := range want {
		if p.Columns[i] != want[i] {
			t.Errorf("column %d = %q, want %q", i, p.Columns[i], want[i])
		}
	}
}

// TestACompositeKeyIsReadInEitherOrder: a predicate is a set of equalities, so requiring the
// declared order would refuse statements that mean precisely the same thing.
func TestACompositeKeyIsReadInEitherOrder(t *testing.T) {
	t.Parallel()

	m := ordersMatcher(t, false)

	for _, q := range []string{
		"SELECT note FROM orders WHERE region = 'eu' AND order_no = 7",
		"SELECT note FROM orders WHERE order_no = 7 AND region = 'eu'",
	} {
		// Literal strings are not a shape this classifier reads — see
		// TestAStringKeyArrivesAsAnArgument — so both of these are refusals, and the point here is
		// that they are refused for the same reason rather than one of them being misread.
		if p := m.Classify(q); p.Kind != proxy.Passthrough {
			t.Errorf("Classify(%q) = %v, want Passthrough", q, p.Kind)
		}
	}

	for _, tc := range []struct {
		q      string
		params [2]int // argument index for region, then for order_no
	}{
		{"SELECT note FROM orders WHERE region = ? AND order_no = ?", [2]int{0, 1}},
		{"SELECT note FROM orders WHERE order_no = ? AND region = ?", [2]int{1, 0}},
	} {
		p := m.Classify(tc.q)
		if p.Kind != proxy.PointSelect {
			t.Errorf("Classify(%q) = %v, want PointSelect", tc.q, p.Kind)
			continue
		}
		if len(p.Key) != 2 {
			t.Errorf("Classify(%q) produced %d key terms, want 2", tc.q, len(p.Key))
			continue
		}
		// Key terms are in DECLARED order; the argument index each one carries is where that
		// column's value was written. Confusing the two would build the key back to front.
		if p.Key[0].Param != tc.params[0] || p.Key[1].Param != tc.params[1] {
			t.Errorf("Classify(%q) takes region from argument %d and order_no from %d, want %d and %d",
				tc.q, p.Key[0].Param, p.Key[1].Param, tc.params[0], tc.params[1])
			continue
		}
		if got := keyOf(t, m, p, args(tc.params, "eu", uint64(7))...); got != "orders:eu:7" {
			t.Errorf("Classify(%q) named %q, want \"orders:eu:7\"", tc.q, got)
		}
	}
}

// args places two values at the argument indexes a statement wrote them at.
func args(params [2]int, region string, orderNo uint64) []any {
	out := make([]any, 2)
	out[params[0]] = region
	out[params[1]] = orderNo
	return out
}

// TestAnIncompleteKeyIsNotAPointRead: naming some of a composite key selects a RANGE, and serving
// one cached row for it would answer a different question.
func TestAnIncompleteKeyIsNotAPointRead(t *testing.T) {
	t.Parallel()

	m := ordersMatcher(t, false)
	for _, q := range []string{
		"SELECT note FROM orders WHERE region = ?",
		"SELECT note FROM orders WHERE order_no = ?",
		"UPDATE orders SET note = ? WHERE region = ?",
		// A repeated column does not complete the key either.
		"SELECT note FROM orders WHERE region = ? AND region = ?",
		// An extra term narrows the selection further than the key does.
		"SELECT note FROM orders WHERE region = ? AND order_no = ? AND note = ?",
	} {
		if p := m.Classify(q); p.Kind == proxy.PointSelect || p.Kind == proxy.PointWrite {
			t.Errorf("Classify(%q) = %v, want a refusal — the key is not fully named", q, p.Kind)
		}
	}
}

// TestAStringKeyArrivesAsAnArgument states a deliberate limitation rather than hiding it.
//
// The predicate grammar admits identifiers, integers and placeholders and nothing else, so a
// quoted literal never reaches the parser. Reading one would mean handling MySQL's escape rules
// inside a matcher whose whole value is that it cannot misread text. Applications overwhelmingly
// send values as arguments, and a literal-keyed statement costs a cache miss rather than a wrong
// answer.
func TestAStringKeyArrivesAsAnArgument(t *testing.T) {
	t.Parallel()

	m := ordersMatcher(t, false)

	if p := m.Classify("SELECT note FROM orders WHERE region = 'eu' AND order_no = 7"); p.Kind != proxy.Passthrough {
		t.Errorf("a quoted literal key was classified as %v", p.Kind)
	}

	p := m.Classify("SELECT note FROM orders WHERE region = ? AND order_no = ?")
	if p.Kind != proxy.PointSelect {
		t.Fatalf("= %v, want PointSelect", p.Kind)
	}
	// The key grammar escapes its values, so a region containing the separator cannot collide with
	// a different row's key.
	if got := keyOf(t, m, p, "e:u", uint64(7)); got != "orders:e%3Au:7" {
		t.Errorf("key = %q, want \"orders:e%%3Au:7\"", got)
	}
}

// TestAnArgumentThatDoesNotFitItsColumnIsRefused: the value MySQL would have rejected or truncated
// does not name the row the caller meant, so the statement goes to the database instead.
func TestAnArgumentThatDoesNotFitItsColumnIsRefused(t *testing.T) {
	t.Parallel()

	m := ordersMatcher(t, false)
	p := m.Classify("SELECT note FROM orders WHERE region = ? AND order_no = ?")
	if p.Kind != proxy.PointSelect {
		t.Fatalf("= %v, want PointSelect", p.Kind)
	}

	for name, a := range map[string][]any{
		"a negative value for an unsigned column": {"eu", int64(-1)},
		"a value that is not a number":            {"eu", "seven"},
		"a float, whose rendering is a choice":    {"eu", 7.0},
		"too few arguments":                       {"eu"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := m.Key(p, a); ok {
				t.Errorf("resolved a key from %v", a)
			}
		})
	}
}

// TestTheVersionBumpUsesTheDeclaredColumn. A deployment whose version column is called something
// else must still have it maintained, and must not have it assigned twice.
func TestTheVersionBumpUsesTheDeclaredColumn(t *testing.T) {
	t.Parallel()

	m := ordersMatcher(t, false)

	got, ok := proxy.RewriteForTest(m, "UPDATE orders SET note = 'x' WHERE region = ? AND order_no = ?")
	if !ok {
		t.Fatal("refused a statement it must be able to rewrite")
	}
	const want = "UPDATE orders SET note = 'x', `row_version` = `row_version` + 1 WHERE region = ? AND order_no = ?"
	if got != want {
		t.Errorf("rewrote to:\n  %s\nwant:\n  %s", got, want)
	}

	// The application maintaining it itself is left alone, under the declared name.
	const own = "UPDATE orders SET note = 'x', row_version = 5 WHERE region = ? AND order_no = ?"
	if got, ok := proxy.RewriteForTest(m, own); !ok || got != own {
		t.Errorf("got %q, ok=%v — a statement that maintains its version column must pass through unchanged", got, ok)
	}

	// A column called "version" is NOT this table's version column, so it is not the assignment
	// the rewrite is looking for and the declared one is still added.
	const decoy = "UPDATE orders SET version = 5 WHERE region = ? AND order_no = ?"
	got, ok = proxy.RewriteForTest(m, decoy)
	if !ok || !strings.Contains(got, "`row_version` = `row_version` + 1") {
		t.Errorf("got %q, ok=%v — the declared version column was not maintained", got, ok)
	}
}

// TestATableWhoseNameStartsTheSameIsNotTheCachedTable. `entities_archive` is somebody else's
// table; matching it would invalidate keys that name rows in a table Cachet does not cache.
func TestATableWhoseNameStartsTheSameIsNotTheCachedTable(t *testing.T) {
	t.Parallel()

	m := entitiesMatcher(t)
	for _, q := range []string{
		"SELECT payload FROM entities_archive WHERE id = 42",
		"UPDATE entities_archive SET status = 1 WHERE id = 7",
		"DELETE FROM entities_archive WHERE id = 7",
		"INSERT INTO entities_archive (id) VALUES (1)",
		"TRUNCATE TABLE entities_archive",
	} {
		if p := m.Classify(q); p.Kind != proxy.Passthrough {
			t.Errorf("Classify(%q) = %v, want Passthrough — that is a different table", q, p.Kind)
		}
	}
}

// TestTheMatcherRefusesAWriteToItsOwnTableWhateverTheShape holds for a non-fixture table too: the
// refusal is what stops a write leaving entries no invalidation will reach.
func TestTheMatcherRefusesAWriteToItsOwnTableWhateverTheShape(t *testing.T) {
	t.Parallel()

	m := ordersMatcher(t, false)
	for _, q := range []string{
		"UPDATE orders SET note = 'x' WHERE region = ?",
		"DELETE FROM orders",
		"INSERT INTO orders (region, order_no) VALUES (?, ?)",
		"REPLACE INTO orders (region) VALUES (?)",
		"TRUNCATE TABLE orders",
	} {
		if p := m.Classify(q); p.Kind != proxy.OpaqueWrite {
			t.Errorf("Classify(%q) = %v, want OpaqueWrite", q, p.Kind)
		}
	}
}
