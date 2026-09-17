// Package admission decides which keys are worth caching, continuously and without a human.
//
// The problem it solves is stated plainly by Uber: caching suits roughly a 20:1 read:write ratio,
// and adoption is opt-in, chosen per use case by a person. Both halves of that are a cost. Ratios
// are not uniform within a table and they drift, so a decision made once is wrong later; and a
// write-churning key inside a read-heavy table is pure loss — every write pays invalidation and
// every read misses anyway.
//
// So the ratio is measured per key rather than assumed per table. The measurement is a count-min
// sketch rather than a map, because tracking a counter per key costs memory proportional to the key
// space, which on the workloads Cachet targets is exactly the thing that does not fit.
package admission

import (
	"hash/fnv"
	"math"
	"sync"
	"time"
)

// SketchOptions configures a Sketch.
type SketchOptions struct {
	// Width and Depth are the sketch dimensions. Memory is Width × Depth × Buckets cells and does
	// not grow with the key space — the property the whole design exists for.
	Width int
	Depth int

	// Window is how far back counts are remembered, and Buckets is how finely it is subdivided.
	//
	// Bucketing matters more than it looks: dropping the whole window at once would make every
	// key's ratio lurch the instant it rolled, and admission decisions would flip with it. Slices
	// expiring one at a time keep the input to the threshold smooth, which is half of why the
	// output does not oscillate.
	Window  time.Duration
	Buckets int

	Now func() time.Time
}

const (
	defaultWidth   = 4096
	defaultDepth   = 4
	defaultWindow  = time.Minute
	defaultBuckets = 6
)

// Sketch estimates per-key read and write counts over a sliding window.
//
// It can OVER-count under hash collision and never under-count, and that asymmetry is the reason it
// is safe to use here. Over-counting writes makes a key look more write-heavy than it is, so
// admission is more conservative and the key goes uncached — the cost is hit rate. Under-counting
// writes would do the opposite: a write-churning key would look read-heavy and stay cached, paying
// invalidation on every write and missing on every read. The error runs in the affordable
// direction by construction.
type Sketch struct {
	width   int
	depth   int
	buckets int
	slice   time.Duration
	now     func() time.Time

	mu sync.Mutex
	// reads and writes are [bucket][depth*width]. head is the bucket currently being written to.
	reads     [][]uint32
	writes    [][]uint32
	head      int
	headStart time.Time
}

// NewSketch builds a Sketch. Non-positive options take defaults, because a sketch that silently
// counted nothing would make every key look unread and admission would cache nothing at all.
func NewSketch(opts SketchOptions) *Sketch {
	if opts.Width <= 0 {
		opts.Width = defaultWidth
	}
	if opts.Depth <= 0 {
		opts.Depth = defaultDepth
	}
	if opts.Window <= 0 {
		opts.Window = defaultWindow
	}
	if opts.Buckets <= 0 {
		opts.Buckets = defaultBuckets
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	s := &Sketch{
		width:     opts.Width,
		depth:     opts.Depth,
		buckets:   opts.Buckets,
		slice:     opts.Window / time.Duration(opts.Buckets),
		now:       opts.Now,
		reads:     make([][]uint32, opts.Buckets),
		writes:    make([][]uint32, opts.Buckets),
		headStart: opts.Now(),
	}
	for i := range s.reads {
		s.reads[i] = make([]uint32, opts.Width*opts.Depth)
		s.writes[i] = make([]uint32, opts.Width*opts.Depth)
	}
	return s
}

// RecordRead counts one read of a key.
func (s *Sketch) RecordRead(key string) { s.record(key, true) }

// RecordWrite counts one write of a key.
func (s *Sketch) RecordWrite(key string) { s.record(key, false) }

func (s *Sketch) record(key string, isRead bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rollLocked()

	table := s.writes[s.head]
	if isRead {
		table = s.reads[s.head]
	}
	for d := 0; d < s.depth; d++ {
		table[s.cell(key, d)]++
	}
}

// Counts estimates a key's reads and writes over the window.
func (s *Sketch) Counts(key string) (reads, writes uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rollLocked()

	return s.estimateLocked(s.reads, key), s.estimateLocked(s.writes, key)
}

// estimateLocked takes the MINIMUM across hash rows, summed over live buckets.
//
// The minimum is what makes this a count-min sketch rather than a pile of collisions: every row's
// counter is at least the true count, so the smallest of them is the closest over-estimate
// available.
func (s *Sketch) estimateLocked(tables [][]uint32, key string) uint32 {
	var total uint32
	for b := 0; b < s.buckets; b++ {
		min := ^uint32(0)
		for d := 0; d < s.depth; d++ {
			if v := tables[b][s.cell(key, d)]; v < min {
				min = v
			}
		}
		total += min
	}
	return total
}

// cell locates a key's counter for one hash row.
func (s *Sketch) cell(key string, row int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	// row is bounded by depth, which New clamps to a small positive number. The mask states that
	// locally so the conversion cannot wrap even if depth is ever made configurable without bound.
	_, _ = h.Write([]byte{byte(row & 0xFF)})
	// SplitMix64's finalizer, for the same reason the routing ring uses it: FNV-1a alone avalanches
	// poorly on near-identical inputs, and "entities:1", "entities:2", … are exactly that. Without
	// it the rows would collide together rather than independently, and the minimum across them
	// would stop being a better estimate than any single row.
	x := h.Sum64()
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	x ^= x >> 31
	// width is clamped positive in New, so the modulus result is always below it and the narrowing
	// cannot wrap. Both the guard and the mask state that locally, so the property is checkable here
	// rather than three functions away.
	width := s.width
	if width <= 0 {
		width = defaultWidth
	}
	slot := x % uint64(width)
	return row*width + int(slot&uint64(math.MaxInt32))
}

// rollLocked advances the ring, clearing buckets that have aged out.
func (s *Sketch) rollLocked() {
	elapsed := s.now().Sub(s.headStart)
	if elapsed < s.slice {
		return
	}

	steps := int(elapsed / s.slice)
	if steps >= s.buckets {
		for b := 0; b < s.buckets; b++ {
			clear(s.reads[b])
			clear(s.writes[b])
		}
		s.head = 0
		s.headStart = s.now()
		return
	}
	for i := 0; i < steps; i++ {
		s.head = (s.head + 1) % s.buckets
		clear(s.reads[s.head])
		clear(s.writes[s.head])
	}
	s.headStart = s.headStart.Add(time.Duration(steps) * s.slice)
}

// Cells is the fixed number of counters, exposed so a test can assert memory does not grow with the
// key space.
func (s *Sketch) Cells() int { return s.width * s.depth }
