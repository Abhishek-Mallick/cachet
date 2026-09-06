// Package cdc is Flux, Cachet's change-data-capture tailer.
//
// Flux streams the MySQL binlog and invalidates cache entries for every row it sees change. It is a
// BACKSTOP, not the primary invalidation path: the engine already tombstones synchronously on the
// write path, before acknowledging the write. Flux catches what that path cannot — writes made
// directly to MySQL, engine instances that crashed between commit and invalidation, and conditional
// updates that were too large to resolve exactly.
//
// It relies on nothing stronger than at-least-once delivery, because every invalidation is a
// compare-and-set: replaying an event that was already applied is a no-op, and replaying an event
// older than the current value loses (CONSISTENCY.md §2).
package cdc

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Position is a place in a shard's binlog.
type Position struct {
	File   string `json:"file"`
	Offset uint32 `json:"offset"`
}

// Before reports whether p precedes other.
//
// Binlog file names are zero-padded and monotonically numbered, so comparing them as strings is
// the same as comparing them numerically.
func (p Position) Before(other Position) bool {
	if p.File != other.File {
		return p.File < other.File
	}
	return p.Offset < other.Offset
}

// String renders the position for logs.
func (p Position) String() string { return fmt.Sprintf("%s:%d", p.File, p.Offset) }

// Checkpoint stores the tailer's progress durably.
type Checkpoint interface {
	Load() (Position, bool, error)
	Save(Position) error
}

// FileCheckpoint persists the binlog position to a file.
//
// A file rather than the cache, deliberately: the cache is configured with no persistence, because
// losing every entry must be a correctness non-event. Losing the CHECKPOINT is not — it would leave
// the tailer resuming from an unknown place. So it needs storage with different properties, and the
// path is expected to live on a volume that survives a restart.
type FileCheckpoint struct {
	path string
}

// NewFileCheckpoint returns a checkpoint stored at path.
func NewFileCheckpoint(path string) *FileCheckpoint { return &FileCheckpoint{path: path} }

// Load reads the stored position. A missing file is a first start, not a failure.
func (c *FileCheckpoint) Load() (Position, bool, error) {
	data, err := os.ReadFile(c.path)
	if errors.Is(err, os.ErrNotExist) {
		return Position{}, false, nil
	}
	if err != nil {
		return Position{}, false, fmt.Errorf("cdc: read checkpoint %s: %w", c.path, err)
	}

	var p Position
	if err := json.Unmarshal(data, &p); err != nil {
		// Starting from scratch on a corrupt checkpoint would create a GAP: every write between the
		// real position and wherever the tailer restarted would never be invalidated, and those
		// keys would stay stale until their TTL with nothing recording that it happened. Refusing
		// to start is the safe failure.
		return Position{}, false, fmt.Errorf("cdc: parse checkpoint %s: %w", c.path, err)
	}
	return p, true, nil
}

// Save writes the position atomically.
//
// Written to a temporary file and renamed, because a checkpoint updated in place can be torn by a
// crash mid-write — leaving a file that parses to a position which never existed, and a tailer that
// resumes from nowhere. Rename is atomic, so a crash leaves either the old checkpoint or the new one.
func (c *FileCheckpoint) Save(p Position) error {
	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("cdc: encode checkpoint: %w", err)
	}

	dir := filepath.Dir(c.path)
	tmp, err := os.CreateTemp(dir, ".flux-checkpoint-*")
	if err != nil {
		return fmt.Errorf("cdc: create temp checkpoint in %s: %w", dir, err)
	}
	tmpName := tmp.Name()

	// Any failure from here on must not leave the temporary file behind, or a crash-looping tailer
	// slowly fills the volume its own checkpoint depends on.
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}

	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return fmt.Errorf("cdc: write temp checkpoint: %w", err)
	}
	// fsync before rename: the rename can otherwise be durable while the contents are not, which is
	// the same torn state the rename was supposed to prevent.
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("cdc: sync temp checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("cdc: close temp checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, c.path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("cdc: rename checkpoint into place: %w", err)
	}
	return nil
}
