package sextant_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/sextant"
)

// Tracing is built before detection, deliberately (build plan §10.5). Detection without tracing
// produces a number nobody can act on: "0.03% of your reads were stale last week" invites exactly
// one question, and a monitor that cannot answer it has told you that you have a problem and
// nothing else.
//
// The trace is what makes Sextant a verifier rather than a monitor. So the constraints that matter
// here are the ones that decide whether it can run in production at all: bounded memory, bounded
// cost per event, and safe under concurrency.

func at(sec int) time.Time { return time.Date(2026, 9, 18, 12, 0, sec, 0, time.UTC) }

func TestATraceRecordsEventsInOrder(t *testing.T) {
	t.Parallel()

	tr := sextant.NewTracer(sextant.TracerOptions{KeysTracked: 10, EventsPerKey: 8})

	tr.Record("entities:1", sextant.Event{Op: sextant.OpFill, Version: 10, At: at(1), Source: sextant.SourceReadFill})
	tr.Record("entities:1", sextant.Event{Op: sextant.OpTombstone, Version: 20, At: at(2), Source: sextant.SourceWritePath})
	tr.Record("entities:1", sextant.Event{Op: sextant.OpFill, Version: 20, At: at(3), Source: sextant.SourceReadFill})

	got := tr.Trace("entities:1")
	if len(got) != 3 {
		t.Fatalf("Trace returned %d events, want 3", len(got))
	}
	// Oldest first. A trace read backwards is a trace that has to be reversed by every consumer,
	// and the one place it is read is an incident.
	for i, want := range []uint64{10, 20, 20} {
		if got[i].Version != want {
			t.Errorf("event %d has version %d, want %d", i, got[i].Version, want)
		}
	}
}

func TestAnUntracedKeyReturnsNothing(t *testing.T) {
	t.Parallel()

	tr := sextant.NewTracer(sextant.TracerOptions{KeysTracked: 10, EventsPerKey: 8})
	if got := tr.Trace("entities:never"); len(got) != 0 {
		t.Errorf("Trace on an unseen key returned %d events, want none", len(got))
	}
}

func TestEventsPerKeyAreBounded(t *testing.T) {
	t.Parallel()

	// A hot key can receive thousands of mutations a second. Without a per-key bound, one key's
	// history is an unbounded allocation in a process that is supposed to be observing quietly.
	tr := sextant.NewTracer(sextant.TracerOptions{KeysTracked: 10, EventsPerKey: 4})

	for i := 1; i <= 20; i++ {
		tr.Record("entities:1", sextant.Event{Op: sextant.OpFill, Version: uint64(i), At: at(i)})
	}

	got := tr.Trace("entities:1")
	if len(got) != 4 {
		t.Fatalf("Trace returned %d events, want the 4 most recent", len(got))
	}
	// The MOST RECENT are kept. When a violation is found, what explains it is what happened just
	// before — the first four events of a key's life are the least useful four.
	for i, want := range []uint64{17, 18, 19, 20} {
		if got[i].Version != want {
			t.Errorf("event %d has version %d, want %d", i, got[i].Version, want)
		}
	}
}

func TestTrackedKeysAreBounded(t *testing.T) {
	t.Parallel()

	// The bound that decides whether this can run against a real workload. A million-key working
	// set must not become a million ring buffers.
	tr := sextant.NewTracer(sextant.TracerOptions{KeysTracked: 3, EventsPerKey: 4})

	for i := 1; i <= 10; i++ {
		tr.Record(fmt.Sprintf("entities:%d", i), sextant.Event{Op: sextant.OpFill, Version: uint64(i), At: at(i)})
	}

	if got := tr.TrackedKeys(); got != 3 {
		t.Errorf("TrackedKeys() = %d, want 3", got)
	}
	// The most recently active keys survive. A key nobody has touched in a while is the one least
	// likely to be the subject of the next investigation.
	for _, key := range []string{"entities:8", "entities:9", "entities:10"} {
		if len(tr.Trace(key)) == 0 {
			t.Errorf("%s was evicted despite being among the most recently active", key)
		}
	}
	if len(tr.Trace("entities:1")) != 0 {
		t.Error("the least recently active key survived eviction")
	}
}

func TestRecordingAgainRefreshesAKey(t *testing.T) {
	t.Parallel()

	tr := sextant.NewTracer(sextant.TracerOptions{KeysTracked: 2, EventsPerKey: 4})

	tr.Record("a", sextant.Event{Op: sextant.OpFill, Version: 1, At: at(1)})
	tr.Record("b", sextant.Event{Op: sextant.OpFill, Version: 2, At: at(2)})
	// Touching "a" again must make "b" the eviction candidate, not "a". Otherwise a steadily active
	// key is evicted on a fixed schedule regardless of activity, which is the opposite of what a
	// recency bound is for.
	tr.Record("a", sextant.Event{Op: sextant.OpFill, Version: 3, At: at(3)})
	tr.Record("c", sextant.Event{Op: sextant.OpFill, Version: 4, At: at(4)})

	if len(tr.Trace("a")) == 0 {
		t.Error("the recently refreshed key was evicted")
	}
	if len(tr.Trace("b")) != 0 {
		t.Error("the least recently touched key survived")
	}
}

func TestATraceIsACopy(t *testing.T) {
	t.Parallel()

	tr := sextant.NewTracer(sextant.TracerOptions{KeysTracked: 4, EventsPerKey: 4})
	tr.Record("a", sextant.Event{Op: sextant.OpFill, Version: 1, At: at(1)})

	got := tr.Trace("a")
	got[0].Version = 999

	// The trace is read during an incident, often by code that is itself being written in a hurry.
	// Handing out the live buffer would let a reader corrupt the evidence it is examining.
	if again := tr.Trace("a"); again[0].Version != 1 {
		t.Errorf("mutating a returned trace changed the stored one: version = %d", again[0].Version)
	}
}

func TestZeroOptionsGetUsableDefaults(t *testing.T) {
	t.Parallel()

	// A zero-valued Tracer that silently recorded nothing would make every trace empty during the
	// one incident anyone needed it for.
	tr := sextant.NewTracer(sextant.TracerOptions{})
	tr.Record("a", sextant.Event{Op: sextant.OpFill, Version: 1, At: at(1)})

	if got := tr.Trace("a"); len(got) != 1 {
		t.Errorf("a default tracer recorded %d events, want 1", len(got))
	}
}

func TestTracingIsSafeUnderConcurrency(t *testing.T) {
	t.Parallel()

	// Sextant observes a live system from several goroutines at once. A tracer that needed external
	// locking would be locked by its callers on the hot path, which is the one place this must cost
	// as little as possible.
	tr := sextant.NewTracer(sextant.TracerOptions{KeysTracked: 32, EventsPerKey: 16})

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("entities:%d", i%40)
				tr.Record(key, sextant.Event{Op: sextant.OpFill, Version: uint64(i), At: at(i % 60)})
				tr.Trace(key)
			}
		}(g)
	}
	wg.Wait()

	if got := tr.TrackedKeys(); got > 32 {
		t.Errorf("TrackedKeys() = %d, over the configured bound of 32", got)
	}
}

func TestEventRendersForAHuman(t *testing.T) {
	t.Parallel()

	// The trace's whole purpose is being read. A line that needs a decoder ring during an incident
	// is a line nobody reads.
	e := sextant.Event{
		Op: sextant.OpTombstone, Version: 117, At: at(5),
		Actor: "engine-2", Source: sextant.SourceCDC,
	}
	got := e.String()
	for _, want := range []string{"tombstone", "117", "engine-2", "cdc"} {
		if !contains(got, want) {
			t.Errorf("Event.String() = %q, missing %q", got, want)
		}
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
