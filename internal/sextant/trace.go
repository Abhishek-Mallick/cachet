// Package sextant is Cachet's consistency verifier.
//
// Every other cache in this category asks to be trusted. Sextant is the part that lets Cachet be
// checked instead: it watches the cache and the database, decides when the difference between them
// is a real violation rather than an in-flight race, and publishes the answer as a number per
// consistency level.
//
// It is built in a deliberate order — tracing first, detection second (build plan §10.5). Detection
// without tracing produces a figure nobody can act on. "0.03% of your reads were stale last week"
// invites exactly one question, and a monitor that cannot answer it has told you that you have a
// problem and nothing else. The trace is what turns the number into something with a cause
// attached, and that difference is the entire product claim.
package sextant

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// Op is a mutation of a cache entry.
type Op string

// The mutations a cache entry can undergo. Recording the lease alongside the data operations is
// deliberate: "nobody filled this because someone else held the lease and died" is a real
// explanation for staleness, and it is invisible if only fills and tombstones are traced.
const (
	OpFill      Op = "fill"
	OpTombstone Op = "tombstone"
	OpEvict     Op = "evict"
	OpLease     Op = "lease"
)

// Source is where a mutation came from.
//
// Knowing which path invalidated an entry is most of the diagnosis: an entry the write path should
// have tombstoned and did not is a different bug from one CDC was late on, and they have different
// fixes.
type Source string

// Where a mutation came from. An entry the write path should have tombstoned and did not is a
// different bug from one CDC was late on, and they have different fixes.
const (
	SourceWritePath Source = "write_path"
	SourceCDC       Source = "cdc"
	SourceReadFill  Source = "read_fill"
	SourceShadow    Source = "shadow"
)

// Event is one recorded mutation.
type Event struct {
	Op      Op
	Version uint64
	At      time.Time
	Actor   string
	Source  Source
}

// String renders an event for a human reading a trace during an incident.
func (e Event) String() string {
	actor := e.Actor
	if actor == "" {
		actor = "-"
	}
	return fmt.Sprintf("%s %-9s v=%d src=%s actor=%s",
		e.At.UTC().Format("15:04:05.000"), e.Op, e.Version, e.Source, actor)
}

// TracerOptions bounds what a Tracer will hold.
type TracerOptions struct {
	// KeysTracked caps how many keys have a trace at all. A million-key working set must not become
	// a million ring buffers in a process whose job is to observe quietly.
	KeysTracked int

	// EventsPerKey caps the history kept for one key. A hot key can take thousands of mutations a
	// second, and without this bound its history is an unbounded allocation.
	EventsPerKey int
}

const (
	defaultKeysTracked  = 10_000
	defaultEventsPerKey = 32
)

// Tracer keeps a bounded, recency-ordered history of mutations per key.
//
// Both bounds are load-bearing rather than defensive. Sextant is meant to run continuously against
// production traffic, so the question "what does this cost when the workload is hostile?" has to
// have an answer that does not depend on the workload. Memory here is O(KeysTracked ×
// EventsPerKey), full stop.
//
// Safe for concurrent use: callers record from the request path, and requiring them to hold a lock
// of their own would put Sextant's bookkeeping on the critical path of the thing it is observing.
type Tracer struct {
	keysTracked  int
	eventsPerKey int

	mu sync.Mutex
	// order is most-recently-active first; entries hold *keyTrace.
	order *list.List
	byKey map[string]*list.Element
}

type keyTrace struct {
	key string
	// events is a ring: next is where the following event goes, filled says how many are valid.
	events []Event
	next   int
	filled int
}

// NewTracer builds a Tracer. Zero or negative bounds take the defaults, because a tracer that
// silently recorded nothing would leave every trace empty during the one incident it existed for.
func NewTracer(opts TracerOptions) *Tracer {
	if opts.KeysTracked <= 0 {
		opts.KeysTracked = defaultKeysTracked
	}
	if opts.EventsPerKey <= 0 {
		opts.EventsPerKey = defaultEventsPerKey
	}
	return &Tracer{
		keysTracked:  opts.KeysTracked,
		eventsPerKey: opts.EventsPerKey,
		order:        list.New(),
		byKey:        make(map[string]*list.Element, opts.KeysTracked),
	}
}

// Record appends a mutation to a key's trace.
func (t *Tracer) Record(key string, e Event) {
	t.mu.Lock()
	defer t.mu.Unlock()

	el, ok := t.byKey[key]
	if ok {
		// Touching a key refreshes it. Without this, a steadily active key is evicted on a fixed
		// schedule regardless of how busy it is — the opposite of what a recency bound is for, and
		// it would drop exactly the keys most likely to be investigated.
		t.order.MoveToFront(el)
	} else {
		if t.order.Len() >= t.keysTracked {
			t.evictOldestLocked()
		}
		el = t.order.PushFront(&keyTrace{key: key, events: make([]Event, t.eventsPerKey)})
		t.byKey[key] = el
	}

	kt, _ := el.Value.(*keyTrace)
	kt.events[kt.next] = e
	kt.next = (kt.next + 1) % len(kt.events)
	if kt.filled < len(kt.events) {
		kt.filled++
	}
}

// Trace returns a key's history, oldest first.
//
// A copy, because the one place a trace is read is an incident, often by code being written in a
// hurry. Handing out the live buffer would let a reader corrupt the evidence it came to examine.
func (t *Tracer) Trace(key string) []Event {
	t.mu.Lock()
	defer t.mu.Unlock()

	el, ok := t.byKey[key]
	if !ok {
		return nil
	}
	kt, _ := el.Value.(*keyTrace)

	out := make([]Event, 0, kt.filled)
	// Walk from the oldest surviving slot. Oldest-first because a trace read backwards has to be
	// reversed by every consumer.
	start := (kt.next - kt.filled + len(kt.events)) % len(kt.events)
	for i := 0; i < kt.filled; i++ {
		out = append(out, kt.events[(start+i)%len(kt.events)])
	}
	return out
}

// TrackedKeys is how many keys currently have a trace.
func (t *Tracer) TrackedKeys() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.order.Len()
}

func (t *Tracer) evictOldestLocked() {
	el := t.order.Back()
	if el == nil {
		return
	}
	kt, _ := el.Value.(*keyTrace)
	delete(t.byKey, kt.key)
	t.order.Remove(el)
}
