package consistency_test

import (
	"fmt"
	"testing"

	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// Session tokens are built, advanced and serialised on every single request, so their allocation
// count is a per-request cost the whole system pays.
func BenchmarkTokenAdvance(b *testing.B) {
	t := consistency.NewToken(consistency.DefaultMaxSessionShards)
	shards := []string{"shard0", "shard1", "shard2"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t.Advance(shards[i%len(shards)], uint64(i))
	}
}

func BenchmarkTokenProto(b *testing.B) {
	t := consistency.NewToken(consistency.DefaultMaxSessionShards)
	for i := 0; i < 8; i++ {
		t.Advance(fmt.Sprintf("shard%d", i), uint64(i+1))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = t.Proto()
	}
}
