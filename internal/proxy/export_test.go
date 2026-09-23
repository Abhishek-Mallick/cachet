package proxy

// RewriteForTest exposes the statement rewrite, which is otherwise reachable only through a live
// connection.
func RewriteForTest(m *Matcher, q string) (string, bool) { return m.rewriteWithVersionBump(q) }
