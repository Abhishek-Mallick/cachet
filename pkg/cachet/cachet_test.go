package cachet_test

import (
	"testing"

	"github.com/Abhishek-Mallick/cachet/pkg/cachet"
)

// The engine's own config writes listeners as "tcp://host:port" and "unix:///path". A user reading
// their cachet.yaml and pasting the address into Dial is doing the obvious thing, and until this
// existed they got "too many colons in address" from deep inside gRPC — an error that says nothing
// about the mistake. unix:// is valid gRPC target syntax and must keep working untouched.
func TestDialAcceptsTheSameAddressSyntaxTheConfigUses(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ in, want string }{
		{"tcp://127.0.0.1:7070", "127.0.0.1:7070"},
		{"tcp://cachet.internal:8080", "cachet.internal:8080"},
		{"127.0.0.1:7070", "127.0.0.1:7070"},
		{"unix:///var/run/cachet.sock", "unix:///var/run/cachet.sock"},
		{"dns:///cachet:8080", "dns:///cachet:8080"},
	} {
		if got := cachet.NormalizeTargetForTest(tc.in); got != tc.want {
			t.Errorf("target %q normalised to %q, want %q", tc.in, got, tc.want)
		}
	}
}
