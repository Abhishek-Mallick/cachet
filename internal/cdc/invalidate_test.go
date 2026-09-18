package cdc_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/cdc"
)

// flakyInvalidator fails its first failures calls, then succeeds.
type flakyInvalidator struct {
	mu       sync.Mutex
	failures int
	calls    int
}

func (f *flakyInvalidator) Tombstone(_ context.Context, _ string, _ uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failures {
		return false, errors.New("cache unreachable")
	}
	return true, nil
}

func (f *flakyInvalidator) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// memCheckpoint records what was saved, so a test can assert the position did NOT move.
type memCheckpoint struct {
	mu    sync.Mutex
	saved cdc.Position
}

func (m *memCheckpoint) Load() (cdc.Position, bool, error) {
	p := m.get()
	return p, p.File != "", nil
}

func (m *memCheckpoint) Save(p cdc.Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saved = p
	return nil
}

func (m *memCheckpoint) get() cdc.Position {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saved
}

// The backstop's whole purpose is to catch what the write path missed. If a cache blip makes it
// drop the event instead, then during a cache partition BOTH invalidation paths fail silently and
// the stale entry survives until its TTL — with nothing reporting it.
func TestATransientCacheFailureIsRetriedRatherThanDropped(t *testing.T) {
	t.Parallel()

	inv := &flakyInvalidator{failures: 2}
	var applied bool
	tailer := cdc.NewForTest(cdc.Options{
		Table:              "entities",
		Cache:              inv,
		Checkpoint:         &memCheckpoint{},
		InvalidateRetryFor: 5 * time.Second,
		OnInvalidate:       func(string, uint64, bool) { applied = true },
	})

	tailer.InvalidateForTest(context.Background(), 42, 7)

	if !applied {
		t.Fatal("the invalidation was dropped after a transient failure; the backstop cannot back-stop a cache outage if one blip loses the event")
	}
	if got := inv.count(); got != 3 {
		t.Errorf("Tombstone called %d times, want 3 (two failures then a success)", got)
	}
	if _, frozen := tailer.FrozenForTest(); frozen {
		t.Error("the checkpoint was frozen even though the invalidation eventually succeeded")
	}
}

// When retries run out, the event must not be abandoned silently. Advancing the checkpoint past an
// invalidation that never landed means a restart cannot recover it either — the staleness becomes
// permanent, bounded only by TTL.
func TestAnUndeliverableInvalidationFreezesTheCheckpoint(t *testing.T) {
	t.Parallel()

	cp := &memCheckpoint{}
	tailer := cdc.NewForTest(cdc.Options{
		Table:              "entities",
		Cache:              &flakyInvalidator{failures: 1 << 30},
		Checkpoint:         cp,
		InvalidateRetryFor: 50 * time.Millisecond,
	})

	before := cdc.Position{File: "binlog.000001", Offset: 100}
	tailer.SetPositionForTest(before)
	tailer.InvalidateForTest(context.Background(), 42, 7)

	// The stream keeps moving; the checkpoint must not follow it past the event that was lost.
	tailer.SetPositionForTest(cdc.Position{File: "binlog.000001", Offset: 900})
	tailer.SaveForTest()

	if got := cp.get(); got != before {
		t.Errorf("checkpoint saved %+v, want it frozen at %+v — replaying from after a lost "+
			"invalidation means a restart cannot recover it either", got, before)
	}
	frozenAt, frozen := tailer.FrozenForTest()
	if !frozen || frozenAt != before {
		t.Errorf("Frozen() = (%+v, %v), want (%+v, true) so an operator can see why the tailer stopped advancing",
			frozenAt, frozen, before)
	}
}
