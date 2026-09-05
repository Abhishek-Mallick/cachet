package engine_test

import (
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/engine"
)

func TestKeyRoundTrips(t *testing.T) {
	t.Parallel()

	k, err := engine.ParseKey("entities:42")
	if err != nil {
		t.Fatalf("ParseKey: %v", err)
	}
	if k.Table != "entities" || k.ID != 42 {
		t.Errorf("ParseKey = %+v, want entities/42", k)
	}
	if got := k.String(); got != "entities:42" {
		t.Errorf("String() = %q, want \"entities:42\"", got)
	}
}

func TestMalformedKeysAreRejected(t *testing.T) {
	t.Parallel()

	// The key is what routing and cache identity are both derived from, so a key the engine
	// half-understands is worse than one it refuses: it would route somewhere deterministic and
	// then be cached under an identity nothing else can reproduce.
	for _, in := range []string{
		"",
		"entities",
		"entities:",
		":42",
		"entities:abc",
		"entities:-1",
		"entities:42:extra",
		"other:42",
	} {
		if _, err := engine.ParseKey(in); err == nil {
			t.Errorf("ParseKey(%q) succeeded; want an error", in)
		}
	}
}

func TestKeyRejectsAnOverflowingID(t *testing.T) {
	t.Parallel()

	if _, err := engine.ParseKey("entities:99999999999999999999999"); err == nil {
		t.Error("ParseKey accepted an id that does not fit a uint64")
	}
}
