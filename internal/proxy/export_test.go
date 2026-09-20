package proxy

// RewriteWithVersionBumpForTest exposes the statement rewrite, which is otherwise reachable only
// through a live connection.
func RewriteWithVersionBumpForTest(q string) (string, bool) { return rewriteWithVersionBump(q) }
