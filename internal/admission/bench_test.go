package admission_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/admission"
)

// Admission sits on the read path, so its cost per read is a latency budget, not an implementation
// detail. This measures it under the concurrency a real engine applies.
func BenchmarkControllerConcurrent(b *testing.B) {
	c := admission.NewController(admission.ControllerOptions{
		Sketch: admission.NewSketch(admission.SketchOptions{Window: time.Minute}),
		Policy: admission.NewPolicy(admission.PolicyOptions{
			AdmitRatio: 20, EvictRatio: 10, MinSamples: 50, DefaultAdmit: true,
		}),
	})
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("entities:%d", i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%len(keys)]
			c.RecordRead(key)
			c.ShouldCache(key)
			i++
		}
	})
}

func BenchmarkSketchCounts(b *testing.B) {
	s := admission.NewSketch(admission.SketchOptions{Window: time.Minute})
	for i := 0; i < 1000; i++ {
		s.RecordRead(fmt.Sprintf("entities:%d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Counts("entities:500")
	}
}

var _ = sync.Mutex{}
