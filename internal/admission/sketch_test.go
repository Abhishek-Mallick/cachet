package admission_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/admission"
)

// A count-min sketch, not a map. The distinction is the whole reason this is usable: tracking a
// read:write ratio per key in a map costs memory proportional to the KEY SPACE, which on the
// workloads Cachet targets is the thing that does not fit. A sketch costs a fixed amount and pays
// for it with over-counting under collision — a trade that is only acceptable because of which
// direction the error runs, which is what these tests pin.

func newClock(t time.Time) *testClock { return &testClock{t: t} }

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func base() time.Time { return time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC) }

func TestAnUnseenKeyCountsZero(t *testing.T) {
	t.Parallel()

	clk := newClock(base())
	s := admission.NewSketch(admission.SketchOptions{Window: time.Minute, Now: clk.Now})

	reads, writes := s.Counts("entities:never")
	if reads != 0 || writes != 0 {
		t.Errorf("Counts on an unseen key = (%d, %d), want (0, 0)", reads, writes)
	}
}

func TestReadsAndWritesAreCountedSeparately(t *testing.T) {
	t.Parallel()

	clk := newClock(base())
	s := admission.NewSketch(admission.SketchOptions{Window: time.Minute, Now: clk.Now})

	for i := 0; i < 20; i++ {
		s.RecordRead("entities:1")
	}
	for i := 0; i < 3; i++ {
		s.RecordWrite("entities:1")
	}

	reads, writes := s.Counts("entities:1")
	if reads < 20 {
		t.Errorf("reads = %d, want at least 20", reads)
	}
	if writes < 3 {
		t.Errorf("writes = %d, want at least 3", writes)
	}
}

func TestTheSketchNeverUNDERCOUNTS(t *testing.T) {
	t.Parallel()

	// The property that makes the whole approach safe. A count-min sketch can OVER-count under hash
	// collision, never under-count.
	//
	// Which direction the error runs decides what a mistake costs. Over-counting writes makes a key
	// look more write-heavy than it is, so admission is more conservative and the key is not cached
	// — costing hit rate. Under-counting writes would make a write-churning key look read-heavy and
	// keep it cached, which costs invalidation work on every write AND a miss on every read. The
	// sketch is chosen precisely because its error runs in the affordable direction.
	clk := newClock(base())
	s := admission.NewSketch(admission.SketchOptions{Width: 64, Depth: 3, Window: time.Minute, Now: clk.Now})

	// Far more keys than the sketch is wide, so collisions are guaranteed.
	for i := 0; i < 2000; i++ {
		s.RecordRead(fmt.Sprintf("entities:%d", i))
	}
	for _, key := range []string{"entities:7", "entities:500", "entities:1999"} {
		if reads, _ := s.Counts(key); reads < 1 {
			t.Errorf("%s counted %d reads after one; the sketch under-counted, which it must never do",
				key, reads)
		}
	}
}

func TestCountsDecayOutOfTheWindow(t *testing.T) {
	t.Parallel()

	// Ratios drift — that is the entire argument for measuring them rather than having a human
	// choose. A sketch that never forgot would hold a key at whatever it was when it was busiest,
	// and adaptive admission would stop adapting after the first hour.
	clk := newClock(base())
	s := admission.NewSketch(admission.SketchOptions{Window: time.Minute, Buckets: 4, Now: clk.Now})

	for i := 0; i < 50; i++ {
		s.RecordRead("entities:1")
	}
	if reads, _ := s.Counts("entities:1"); reads == 0 {
		t.Fatal("precondition: reads should be counted")
	}

	clk.advance(2 * time.Minute)
	if reads, writes := s.Counts("entities:1"); reads != 0 || writes != 0 {
		t.Errorf("Counts after the window elapsed = (%d, %d), want (0, 0)", reads, writes)
	}
}

func TestDecayIsGradualNotACliff(t *testing.T) {
	t.Parallel()

	// Dropping the whole window at once would make every key's ratio lurch the instant the window
	// rolled, and admission would flip with it. Bucketing means old counts leave a slice at a time.
	//
	// Note what "gradual" applies to: SUSTAINED traffic. A single burst lands in one bucket and
	// necessarily leaves in one piece, a full window later — that is a property of the ring, not a
	// flaw. What must not happen is a steady stream of traffic vanishing all at once, because a
	// steady stream is what a real key looks like.
	clk := newClock(base())
	s := admission.NewSketch(admission.SketchOptions{Window: time.Minute, Buckets: 4, Now: clk.Now})

	// Traffic spread across the whole window, one slice at a time.
	for slice := 0; slice < 4; slice++ {
		for i := 0; i < 25; i++ {
			s.RecordRead("entities:1")
		}
		clk.advance(15 * time.Second)
	}
	full, _ := s.Counts("entities:1")
	if full == 0 {
		t.Fatal("precondition: sustained traffic should be counted")
	}

	// Now let it age with no new traffic. Counts must come down in steps rather than to zero at
	// once — each observation strictly smaller, none of them the whole drop.
	prev := full
	drops := 0
	for slice := 0; slice < 4; slice++ {
		clk.advance(15 * time.Second)
		got, _ := s.Counts("entities:1")
		if got > prev {
			t.Fatalf("counts rose with no traffic: %d then %d", prev, got)
		}
		if got < prev {
			drops++
		}
		prev = got
	}

	if drops < 2 {
		t.Errorf("counts fell in %d step(s) over four slices; decay is a cliff rather than a slope", drops)
	}
	if prev != 0 {
		t.Errorf("counts = %d after a full window with no traffic, want 0", prev)
	}
}

func TestZeroOptionsGetUsableDefaults(t *testing.T) {
	t.Parallel()

	// A zero-valued sketch that silently counted nothing would make every key look unread, and
	// admission would refuse to cache anything at all.
	s := admission.NewSketch(admission.SketchOptions{})
	s.RecordRead("entities:1")

	if reads, _ := s.Counts("entities:1"); reads < 1 {
		t.Errorf("a default sketch counted %d reads, want at least 1", reads)
	}
}

func TestMemoryDoesNotGrowWithTheKeySpace(t *testing.T) {
	t.Parallel()

	// The reason this is a sketch. A million distinct keys must cost the same as ten.
	s := admission.NewSketch(admission.SketchOptions{Width: 128, Depth: 4, Window: time.Minute})

	for i := 0; i < 100_000; i++ {
		s.RecordRead(fmt.Sprintf("entities:%d", i))
	}
	if got := s.Cells(); got != 128*4 {
		t.Errorf("Cells() = %d after 100,000 distinct keys, want the fixed 512", got)
	}
}

func TestTheSketchIsSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	// Recorded from the request path by every in-flight request at once.
	clk := newClock(base())
	s := admission.NewSketch(admission.SketchOptions{Window: time.Minute, Now: clk.Now})

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("entities:%d", i%50)
				if i%10 == 0 {
					s.RecordWrite(key)
				} else {
					s.RecordRead(key)
				}
				s.Counts(key)
			}
		}(g)
	}
	wg.Wait()

	if reads, writes := s.Counts("entities:1"); reads == 0 && writes == 0 {
		t.Error("nothing was counted after 8000 concurrent operations")
	}
}
