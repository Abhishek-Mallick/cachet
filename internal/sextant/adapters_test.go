package sextant_test

import (
	"fmt"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/sextant"
)

func TestRecentKeysReturnsNothingWhenEmpty(t *testing.T) {
	t.Parallel()

	// A verifier started before any traffic must sample nothing rather than block or invent a key.
	r := sextant.NewRecentKeys(10)
	if _, ok := r.Next(); ok {
		t.Error("Next() returned a key from an empty source")
	}
}

func TestRecentKeysIsBounded(t *testing.T) {
	t.Parallel()

	// This runs beside a live system, so it must not grow with the workload. A million-key write
	// burst must not become a million-entry slice in the observer.
	r := sextant.NewRecentKeys(5)
	for i := 0; i < 100; i++ {
		r.Touch(fmt.Sprintf("entities:%d", i))
	}
	if got := r.Len(); got != 5 {
		t.Errorf("Len() = %d, want the configured bound of 5", got)
	}
}

func TestRecentKeysKeepsTheMostRecent(t *testing.T) {
	t.Parallel()

	// A key written long ago has either converged or been reported already, so it is the cheapest
	// one to stop watching. Dropping the newest instead would make the source blind to exactly the
	// writes most likely to still be propagating.
	r := sextant.NewRecentKeys(3)
	for i := 1; i <= 6; i++ {
		r.Touch(fmt.Sprintf("entities:%d", i))
	}

	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		if k, ok := r.Next(); ok {
			seen[k] = true
		}
	}
	for _, want := range []string{"entities:4", "entities:5", "entities:6"} {
		if !seen[want] {
			t.Errorf("%s should still be a candidate but was never sampled", want)
		}
	}
	for _, gone := range []string{"entities:1", "entities:2", "entities:3"} {
		if seen[gone] {
			t.Errorf("%s was evicted but is still being sampled", gone)
		}
	}
}

func TestTouchingAKeyTwiceDoesNotDuplicateIt(t *testing.T) {
	t.Parallel()

	// A hot key is written constantly. Without de-duplication it would crowd out every other
	// candidate and the verifier would sample one key forever.
	r := sextant.NewRecentKeys(10)
	for i := 0; i < 50; i++ {
		r.Touch("entities:1")
	}
	r.Touch("entities:2")

	if got := r.Len(); got != 2 {
		t.Errorf("Len() = %d after touching two distinct keys, want 2", got)
	}
}

func TestRecentKeysSamplesEveryCandidate(t *testing.T) {
	t.Parallel()

	// Random rather than round-robin, so a key that is stale only intermittently is still eventually
	// sampled. A fixed order can synchronise with a periodic workload and miss the same key every
	// round forever — a blind spot that would never show up as an error.
	r := sextant.NewRecentKeys(10)
	for i := 0; i < 5; i++ {
		r.Touch(fmt.Sprintf("entities:%d", i))
	}

	seen := map[string]bool{}
	for i := 0; i < 500; i++ {
		if k, ok := r.Next(); ok {
			seen[k] = true
		}
	}
	if len(seen) != 5 {
		t.Errorf("sampled %d of 5 candidates over 500 draws", len(seen))
	}
}
