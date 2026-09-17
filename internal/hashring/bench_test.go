package hashring_test

import (
	"fmt"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/hashring"
)

// Ring lookup runs on every request, twice — once to route to a shard and once to a cache node. It
// is pure CPU with no external dependency, which makes it exactly the kind of thing a shared CI
// runner can measure usefully.
func BenchmarkRingLookup(b *testing.B) {
	r := hashring.NewRing("shard0", "shard1", "shard2")
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("entities:%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.Lookup(keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}
