package cdc_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/cdc"
)

func TestCheckpointRoundTrips(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "flux.pos")
	store := cdc.NewFileCheckpoint(path)

	want := cdc.Position{File: "binlog.000007", Offset: 4823}
	if err := store.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, found, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Fatal("Load reported no checkpoint after Save")
	}
	if got != want {
		t.Errorf("Load = %+v, want %+v", got, want)
	}
}

func TestMissingCheckpointIsNotAnError(t *testing.T) {
	t.Parallel()

	store := cdc.NewFileCheckpoint(filepath.Join(t.TempDir(), "absent.pos"))

	// A first start has no checkpoint. Treating that as a failure would mean the tailer could never
	// start for the first time.
	_, found, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if found {
		t.Error("Load reported a checkpoint that was never written")
	}
}

func TestCheckpointSurvivesOverwrite(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "flux.pos")
	store := cdc.NewFileCheckpoint(path)

	for _, p := range []cdc.Position{
		{File: "binlog.000001", Offset: 100},
		{File: "binlog.000001", Offset: 200},
		{File: "binlog.000002", Offset: 4},
	} {
		if err := store.Save(p); err != nil {
			t.Fatalf("Save %+v: %v", p, err)
		}
		got, _, err := store.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got != p {
			t.Fatalf("Load = %+v, want %+v", got, p)
		}
	}
}

func TestSaveIsAtomic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "flux.pos")
	store := cdc.NewFileCheckpoint(path)

	if err := store.Save(cdc.Position{File: "binlog.000001", Offset: 100}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// A checkpoint written in place can be torn by a crash mid-write, leaving a file that parses to
	// a position that never existed — and the tailer would then resume from nowhere. Writing to a
	// temporary file and renaming makes the update atomic, so a crash leaves either the old
	// checkpoint or the new one.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("Save left %d files behind (%v); the temporary file was not cleaned up", len(entries), names)
	}
}

func TestCorruptCheckpointIsReportedNotIgnored(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "flux.pos")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Silently starting from scratch would create a GAP: every write between the real position and
	// wherever the tailer restarted would never be invalidated, and those keys would stay stale
	// until their TTL. Refusing to start is the safe failure.
	if _, _, err := cdc.NewFileCheckpoint(path).Load(); err == nil {
		t.Error("Load accepted a corrupt checkpoint instead of refusing to start")
	}
}

func TestPositionOrdering(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		a, b     cdc.Position
		aBeforeB bool
	}{
		{"same file, lower offset", cdc.Position{File: "b.000001", Offset: 4}, cdc.Position{File: "b.000001", Offset: 100}, true},
		{"same file, same offset", cdc.Position{File: "b.000001", Offset: 100}, cdc.Position{File: "b.000001", Offset: 100}, false},
		{"earlier file", cdc.Position{File: "b.000001", Offset: 999}, cdc.Position{File: "b.000002", Offset: 4}, true},
		{"later file", cdc.Position{File: "b.000003", Offset: 4}, cdc.Position{File: "b.000002", Offset: 999}, false},
	} {
		if got := tc.a.Before(tc.b); got != tc.aBeforeB {
			t.Errorf("%s: %+v.Before(%+v) = %v, want %v", tc.name, tc.a, tc.b, got, tc.aBeforeB)
		}
	}
}
