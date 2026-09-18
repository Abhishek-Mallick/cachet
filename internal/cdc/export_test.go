package cdc

import (
	"context"
	"io"
	"log/slog"
)

// Test seams for the invalidation path.
//
// New connects to a database, which the retry and checkpoint-freezing logic has no need of: that
// logic is about what happens when the CACHE is unreachable. Building a Tailer directly lets those
// rules be tested as units rather than only through a container.

func NewForTest(opts Options) *Tailer {
	if opts.CheckpointEvery <= 0 {
		opts.CheckpointEvery = defaultCheckpointEvery
	}
	if opts.InvalidateRetryFor == 0 {
		opts.InvalidateRetryFor = defaultInvalidateRetryFor
	}
	return &Tailer{opts: opts, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func (t *Tailer) InvalidateForTest(ctx context.Context, id, version uint64) {
	t.invalidate(ctx, id, version)
}

func (t *Tailer) SetPositionForTest(p Position) { t.setPosition(p) }

func (t *Tailer) SaveForTest() { t.saveIfDirty() }

func (t *Tailer) FrozenForTest() (Position, bool) { return t.Frozen() }
