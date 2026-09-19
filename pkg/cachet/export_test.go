package cachet

// NormalizeTargetForTest exposes the dial-target rewrite, which has no other observable effect
// until a connection is attempted.
func NormalizeTargetForTest(target string) string { return normalizeTarget(target) }
