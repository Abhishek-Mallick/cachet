package proxy_test

import (
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/proxy"
)

const table = "entities"

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
		p := proxy.Classify(table, q)
		if p.Kind != proxy.PointSelect {
			t.Errorf("Classify(%q) = %v, want PointSelect", q, p.Kind)
			continue
		}
		if p.ID != 42 {
			t.Errorf("Classify(%q) extracted id %d, want 42", q, p.ID)
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

	p := proxy.Classify(table, "SELECT status, id, payload FROM entities WHERE id = 42")
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
		if p := proxy.Classify(table, q); p.Kind != proxy.Passthrough {
			t.Errorf("Classify(%q) = %v, want Passthrough — the classifier was clever where it "+
				"had no proof", q, p.Kind)
		}
	}
}

func TestAPointWriteNamesTheRowItTouches(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		q  string
		id uint64
	}{
		{"UPDATE entities SET status = 1 WHERE id = 7", 7},
		{"update entities set status = 1, payload = 'x' where id = 7", 7},
		{"DELETE FROM entities WHERE id = 7", 7},
		{"delete from `entities` where `id` = 7;", 7},
	} {
		p := proxy.Classify(table, tc.q)
		if p.Kind != proxy.PointWrite {
			t.Errorf("Classify(%q) = %v, want PointWrite", tc.q, p.Kind)
			continue
		}
		if p.ID != tc.id {
			t.Errorf("Classify(%q) extracted id %d, want %d", tc.q, p.ID, tc.id)
		}
	}
}

// A write to the cached table that the classifier cannot pin to specific rows is the dangerous
// case: passing it through silently would leave stale entries behind. It must be reported so the
// caller can decide, not quietly treated as harmless.
func TestAnUnrecognisedWriteToTheCachedTableIsReported(t *testing.T) {
	t.Parallel()

	for _, q := range []string{
		"UPDATE entities SET status = 1 WHERE tenant_id = 9",
		"UPDATE entities SET status = 1",
		"DELETE FROM entities WHERE tenant_id = 9",
		"DELETE FROM entities",
		"INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (1, 2, 3, 'x', 4)",
		"REPLACE INTO entities (id) VALUES (1)",
		"TRUNCATE TABLE entities",
	} {
		if p := proxy.Classify(table, q); p.Kind != proxy.OpaqueWrite {
			t.Errorf("Classify(%q) = %v, want OpaqueWrite — an unpinnable write to the cached "+
				"table cannot be treated as harmless", q, p.Kind)
		}
	}
}

// A write to a table nobody caches is genuinely none of the proxy's business.
func TestAWriteToAnotherTableIsPassedThrough(t *testing.T) {
	t.Parallel()

	for _, q := range []string{
		"UPDATE orders SET status = 1 WHERE id = 7",
		"DELETE FROM audit_log WHERE id = 7",
		"INSERT INTO orders (id) VALUES (1)",
	} {
		if p := proxy.Classify(table, q); p.Kind != proxy.Passthrough {
			t.Errorf("Classify(%q) = %v, want Passthrough", q, p.Kind)
		}
	}
}

// Transaction control has to be visible to the proxy even though it forwards it: inside a
// transaction, a cached read can contradict what the transaction has already written.
func TestTransactionControlIsRecognised(t *testing.T) {
	t.Parallel()

	for _, q := range []string{"BEGIN", "begin", "START TRANSACTION", "start transaction  ;"} {
		if p := proxy.Classify(table, q); p.Kind != proxy.Passthrough || !p.BeginsTransaction {
			t.Errorf("Classify(%q) did not report the start of a transaction", q)
		}
	}
	for _, q := range []string{"COMMIT", "commit;", "ROLLBACK", "rollback  "} {
		if p := proxy.Classify(table, q); p.Kind != proxy.Passthrough || !p.EndsTransaction {
			t.Errorf("Classify(%q) did not report the end of a transaction", q)
		}
	}
}

// Rewriting a statement is the most dangerous thing this package does: the result is executed
// against the caller's database. It must change exactly one thing — the version column — and
// refuse anything it cannot change safely.
func TestTheVersionBumpIsAddedToASingleRowUpdate(t *testing.T) {
	t.Parallel()

	got, ok := proxy.RewriteWithVersionBumpForTest("UPDATE entities SET status = 1 WHERE id = 7")
	if !ok {
		t.Fatal("refused a statement it must be able to rewrite")
	}
	if got != "UPDATE entities SET status = 1, `version` = `version` + 1 WHERE id = 7" {
		t.Errorf("rewrote to:\n  %s", got)
	}
}

func TestADeleteNeedsNoRewrite(t *testing.T) {
	t.Parallel()

	const q = "DELETE FROM entities WHERE id = 7"
	got, ok := proxy.RewriteWithVersionBumpForTest(q)
	if !ok || got != q {
		t.Errorf("a delete removes the row, so there is no version left to bump; got %q, ok=%v", got, ok)
	}
}

// A statement that already sets the version is the application maintaining it itself. Adding a
// second assignment would be ambiguous at best and wrong at worst, so it is left alone.
func TestAStatementThatAlreadySetsVersionIsLeftAlone(t *testing.T) {
	t.Parallel()

	const q = "UPDATE entities SET status = 1, version = 99 WHERE id = 7"
	got, ok := proxy.RewriteWithVersionBumpForTest(q)
	if !ok || got != q {
		t.Errorf("got %q, ok=%v — a statement that maintains version itself must pass through unchanged", got, ok)
	}
}

// Anything that could hide a second statement or a string literal containing SQL is refused
// outright. The rewrite appends text to a statement; appending to something misunderstood is how a
// proxy turns a cache into an incident.
func TestTheRewriteRefusesWhatItCannotSeeThrough(t *testing.T) {
	t.Parallel()

	// String literals and quoted identifiers are resolved by the scanner rather than refused —
	// see TestTheRewriteHandlesStringLiterals. What stays refused is text that does not say what it
	// appears to say.
	for _, q := range []string{
		"UPDATE entities SET status = 1 WHERE id = 7; DROP TABLE entities",
		"UPDATE entities SET status = 1 /* where */ WHERE id = 7",
	} {
		if _, ok := proxy.RewriteWithVersionBumpForTest(q); ok {
			t.Errorf("rewrote a statement it should have refused: %q", q)
		}
	}
}

// Setting a string value is the most ordinary update there is, and the rewrite has to handle it —
// including when the string itself contains SQL keywords, quotes, or escapes.
func TestTheRewriteHandlesStringLiterals(t *testing.T) {
	t.Parallel()

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
		got, ok := proxy.RewriteWithVersionBumpForTest(tc.in)
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

	for _, q := range []string{
		"UPDATE entities SET status = 1 WHERE id = 7; DROP TABLE entities",
		"UPDATE entities SET status = 1 /* where */ WHERE id = 7",
		"UPDATE entities SET status = 1 -- x\n WHERE id = 7",
		"UPDATE entities SET payload = 'unterminated WHERE id = 7",
		"UPDATE entities SET status = 1",                         // no predicate at all
		"UPDATE entities SET status = (SELECT 1 FROM t WHERE x)", // only a nested where
	} {
		if got, ok := proxy.RewriteWithVersionBumpForTest(q); ok {
			t.Errorf("rewrote a statement it should have refused:\n  %q\n  → %q", q, got)
		}
	}
}

// Prepared statements carry their values as arguments, so the classifier has to recognise the
// placeholder and say WHICH argument supplies the id.
func TestAPlaceholderIdIsRecognised(t *testing.T) {
	t.Parallel()

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
		p := proxy.Classify(table, tc.q)
		if p.Kind != tc.kind {
			t.Errorf("Classify(%q) = %v, want %v", tc.q, p.Kind, tc.kind)
			continue
		}
		if !p.IDIsParam {
			t.Errorf("Classify(%q) did not report the id as a parameter", tc.q)
			continue
		}
		if p.IDParam != tc.param {
			t.Errorf("Classify(%q) says argument %d supplies the id, want %d", tc.q, p.IDParam, tc.param)
		}
	}
}

// A placeholder anywhere the classifier cannot account for means it does not know what the
// statement will do when executed, so it refuses to claim anything about it.
func TestAPlaceholderItCannotAccountForIsPassedThrough(t *testing.T) {
	t.Parallel()

	for _, q := range []string{
		"SELECT payload FROM entities WHERE id = ? AND tenant_id = ?",
		"SELECT payload FROM entities WHERE tenant_id = ?",
		"UPDATE entities SET status = 1 WHERE tenant_id = ?",
		"SELECT payload FROM entities WHERE id = ? LIMIT ?",
	} {
		p := proxy.Classify(table, q)
		if p.Kind == proxy.PointSelect || p.Kind == proxy.PointWrite {
			t.Errorf("Classify(%q) = %v, want Passthrough or OpaqueWrite", q, p.Kind)
		}
	}
}

// A literal id must not be reported as a parameter, or the handler would look for an argument that
// does not exist.
func TestALiteralIdIsNotAParameter(t *testing.T) {
	t.Parallel()

	if p := proxy.Classify(table, "SELECT payload FROM entities WHERE id = 42"); p.IDIsParam {
		t.Error("a literal id was reported as a parameter")
	}
}
