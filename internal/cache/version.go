package cache

import (
	"fmt"
	"strconv"
)

// versionDigits is the fixed width used to encode a version for Lua.
//
// A uint64 is at most 20 decimal digits, so zero-padding to 20 makes lexicographic string order
// identical to numeric order — which is the whole point.
const versionDigits = 20

// encodeVersion renders a version as a fixed-width zero-padded decimal string.
//
// Versions are compared inside Lua, and Lua numbers are IEEE doubles with a 53-bit mantissa while
// an HLC version is a full uint64. Passing one through tonumber() silently rounds it, so two
// adjacent versions can compare EQUAL — and a stale fill would then win a compare-and-set. Real
// versions are far above 2^53 (physical milliseconds are ~1.7e12, shifted left 16 bits), so this is
// not a theoretical concern.
//
// Fixed-width zero padding sidesteps the problem entirely: the scripts compare strings and never
// convert. TestVersionsNearTheDoublePrecisionLimitCompareExactly fails if that ever changes.
func encodeVersion(v uint64) string {
	return fmt.Sprintf("%0*d", versionDigits, v)
}

// decodeVersion parses a version written by encodeVersion.
func decodeVersion(s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: bad version %q", ErrCorruptEntry, s)
	}
	return v, nil
}
