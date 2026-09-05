package harness

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
)

// KeyFor renders a row id as a Cachet key. The benchmark, the seeder and the engine must agree on
// this format or the load generator will read keys that were never written.
func KeyFor(id uint64) string {
	// Row ids are 1-based in the fixture; the generators produce 0-based indices.
	return "entities:" + strconv.FormatUint(id+1, 10)
}

// Generator produces row indices for a workload.
type Generator interface {
	Next() uint64
}

// Zipfian draws indices from a Zipf distribution: a few keys take most of the traffic, most keys
// are almost never touched.
//
// This is the standard workload (θ=0.99, the YCSB default) and not a stylistic choice. Uniform keys
// over ten million rows mean almost no key is ever hot: the hit rate collapses toward zero, every
// caching effect disappears, and a stampede — the thing leases exist to fix — becomes impossible to
// produce (benchmarking doc §3.5).
//
// A Zipfian is safe for concurrent use, because the open-loop driver runs many workers against one
// generator.
type Zipfian struct {
	items uint64
	alpha float64
	zetan float64
	eta   float64
	theta float64

	mu  sync.Mutex
	rng *rand.Rand
}

// NewZipfian builds a generator over items indices with the given skew.
//
// theta must be in [0,1): the exponent 1/(1-theta) is undefined at exactly 1, and values above it
// invert the distribution.
func NewZipfian(items uint64, theta float64, seed int64) (*Zipfian, error) {
	if items == 0 {
		return nil, fmt.Errorf("harness: zipfian needs at least one item")
	}
	if theta < 0 || theta >= 1 {
		return nil, fmt.Errorf("harness: zipfian theta must be in [0,1), got %v", theta)
	}

	zetan := zeta(items, theta)
	zeta2 := zeta(2, theta)

	return &Zipfian{
		items: items,
		theta: theta,
		alpha: 1 / (1 - theta),
		zetan: zetan,
		eta:   (1 - math.Pow(2.0/float64(items), 1-theta)) / (1 - zeta2/zetan),
		rng:   rand.New(rand.NewSource(seed)),
	}, nil
}

// Next returns the next index.
func (z *Zipfian) Next() uint64 {
	z.mu.Lock()
	u := z.rng.Float64()
	z.mu.Unlock()

	uz := u * z.zetan
	var rank uint64
	switch {
	case uz < 1:
		rank = 0
	case uz < 1+math.Pow(0.5, z.theta):
		rank = 1
	default:
		rank = uint64(float64(z.items) * math.Pow(z.eta*u-z.eta+1, z.alpha))
	}
	if rank >= z.items {
		rank = z.items - 1
	}

	// Scramble the rank into the key space.
	//
	// A textbook Zipfian makes the LOWEST-numbered ids the hottest, so the working set would always
	// be rows 1..N — physically adjacent in an LSM engine, sharing SST blocks and block-cache
	// pages. That hands the storage engine a locality advantage no real workload has, and it would
	// flatter the uncached baseline this project measures everything else against.
	//
	// Hashing preserves the frequency distribution exactly — rank r is still drawn exactly as often
	// — while scattering the hot rows across the id space.
	//
	// Note what this does NOT do: it does not even out per-shard load. Under θ=0.99 a single key
	// can take several percent of all traffic, so whichever shard owns it runs hotter. That is a
	// true property of skewed workloads rather than an artifact, and smoothing it away would make
	// the benchmark less realistic, not more.
	return scramble(rank) % z.items
}

// Uniform draws indices with equal probability. It backs W1, the control workload: it exists to
// show that the caching effects seen under W2 come from skew rather than from the harness, so it is
// never a headline number.
type Uniform struct {
	items uint64

	mu  sync.Mutex
	rng *rand.Rand
}

// NewUniform builds a uniform generator over items indices.
func NewUniform(items uint64, seed int64) *Uniform {
	return &Uniform{
		items: items,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

// Next returns the next index.
func (u *Uniform) Next() uint64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return uint64(u.rng.Int63n(int64(u.items)))
}

// Hot always returns the same index. It backs W3, the stampede workload: ten thousand concurrent
// readers on one key immediately after it is invalidated.
type Hot struct{ id uint64 }

// NewHot builds a single-key generator.
func NewHot(id uint64) *Hot { return &Hot{id: id} }

// Next returns the hot index.
func (h *Hot) Next() uint64 { return h.id }

// zeta computes the generalised harmonic number sum(1/i^theta) for i in [1,n].
//
// It is O(n) and runs once at construction. For a ten-million-row fixture that is a few tens of
// milliseconds, paid once per run rather than per request.
func zeta(n uint64, theta float64) float64 {
	var sum float64
	for i := uint64(1); i <= n; i++ {
		sum += 1 / math.Pow(float64(i), theta)
	}
	return sum
}

// scramble spreads ranks across the key space with a SplitMix64 finalizer. It is a bijection, so it
// relabels keys without altering how often each rank is drawn.
func scramble(v uint64) uint64 {
	v += 0x9E3779B97F4A7C15
	v = (v ^ (v >> 30)) * 0xBF58476D1CE4E5B9
	v = (v ^ (v >> 27)) * 0x94D049BB133111EB
	return v ^ (v >> 31)
}
