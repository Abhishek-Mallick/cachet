package cache

import "testing"

// Version encoding is unit-tested in the package rather than through the client, because its whole
// purpose is a property of the ENCODING — that lexicographic order matches numeric order — and that
// can be checked without a cache server.

func TestEncodedVersionsSortLikeNumbers(t *testing.T) {
	t.Parallel()

	ascending := []uint64{
		0, 1, 2,
		1 << 52,
		1<<53 - 1, 1 << 53, 1<<53 + 1, 1<<53 + 2, // straddles the IEEE double mantissa limit
		1 << 62,
		^uint64(0) - 1, ^uint64(0),
	}

	for i := 1; i < len(ascending); i++ {
		lo, hi := encodeVersion(ascending[i-1]), encodeVersion(ascending[i])
		// The Lua scripts compare these as strings and never call tonumber(). If padding were wrong
		// or width varied, a stale fill could win its compare-and-set — silent staleness.
		if lo >= hi {
			t.Errorf("encodeVersion(%d)=%q does not sort before encodeVersion(%d)=%q",
				ascending[i-1], lo, ascending[i], hi)
		}
	}
}

func TestEncodedVersionsAreFixedWidth(t *testing.T) {
	t.Parallel()

	// Lexicographic order only matches numeric order when every value has the same width. A single
	// short string would silently break comparisons for that key.
	for _, v := range []uint64{0, 1, 1 << 32, ^uint64(0)} {
		if got := len(encodeVersion(v)); got != versionDigits {
			t.Errorf("encodeVersion(%d) is %d chars, want %d", v, got, versionDigits)
		}
	}
}

func TestVersionRoundTrips(t *testing.T) {
	t.Parallel()

	for _, v := range []uint64{0, 1, 1<<53 + 1, ^uint64(0)} {
		got, err := decodeVersion(encodeVersion(v))
		if err != nil {
			t.Errorf("decodeVersion(encodeVersion(%d)): %v", v, err)
			continue
		}
		if got != v {
			t.Errorf("round trip of %d produced %d", v, got)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	t.Parallel()

	for _, s := range []string{"", "abc", "-1", "00000000000000000000x"} {
		if _, err := decodeVersion(s); err == nil {
			t.Errorf("decodeVersion(%q) succeeded", s)
		}
	}
}
