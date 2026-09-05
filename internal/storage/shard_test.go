package storage_test

import (
	"errors"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

func TestRouterRejectsAnEmptyTopology(t *testing.T) {
	t.Parallel()

	_, err := storage.NewRouter(nil)
	if !errors.Is(err, storage.ErrNoNodes) {
		t.Errorf("NewRouter(nil) returned %v, want ErrNoNodes", err)
	}
}

func TestRouterResolvesAKeyToItsShard(t *testing.T) {
	t.Parallel()

	r, err := storage.NewRouter([]storage.ShardID{"shard0", "shard1", "shard2"})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	id, err := r.ShardFor("entities:42")
	if err != nil {
		t.Fatalf("ShardFor: %v", err)
	}
	if !r.Has(id) {
		t.Errorf("ShardFor returned %q, which is not a member of the topology %v", id, r.Shards())
	}
}

func TestRouterGroupsKeysByShardPreservingOrder(t *testing.T) {
	t.Parallel()

	r, err := storage.NewRouter([]storage.ShardID{"shard0", "shard1", "shard2"})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	// BatchGet fans out one query per shard rather than one per key. Grouping is what turns N round
	// trips into at most len(shards) — the difference between a batch API and a loop.
	in := []string{"entities:1", "entities:2", "entities:3", "entities:4", "entities:5", "entities:6"}
	groups, err := r.Group(in)
	if err != nil {
		t.Fatalf("Group: %v", err)
	}

	seen := 0
	for id, ks := range groups {
		if !r.Has(id) {
			t.Errorf("group keyed by unknown shard %q", id)
		}
		for _, k := range ks {
			got, err := r.ShardFor(k)
			if err != nil {
				t.Fatalf("ShardFor(%q): %v", k, err)
			}
			if got != id {
				t.Errorf("key %q grouped under %q but routes to %q", k, id, got)
			}
		}
		// Within a group, keys must stay in their input order so a caller can zip results back
		// against its own request slice without sorting.
		for i := 1; i < len(ks); i++ {
			if indexOf(in, ks[i-1]) > indexOf(in, ks[i]) {
				t.Errorf("group %q reordered keys: %v", id, ks)
				break
			}
		}
		seen += len(ks)
	}
	if seen != len(in) {
		t.Errorf("grouping covered %d keys, want %d", seen, len(in))
	}
}

func TestRouterGroupDeduplicatesKeys(t *testing.T) {
	t.Parallel()

	r, err := storage.NewRouter([]storage.ShardID{"shard0", "shard1"})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	groups, err := r.Group([]string{"entities:7", "entities:7", "entities:7"})
	if err != nil {
		t.Fatalf("Group: %v", err)
	}

	total := 0
	for _, ks := range groups {
		total += len(ks)
	}
	if total != 1 {
		t.Errorf("Group emitted %d keys for one repeated key, want 1", total)
	}
}

func TestRouterRejectsAnEmptyKey(t *testing.T) {
	t.Parallel()

	r, err := storage.NewRouter([]storage.ShardID{"shard0"})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}

	// An empty key would route somewhere deterministic and be silently cacheable, which is a bug
	// that surfaces as mysterious cross-talk rather than as an error.
	if _, err := r.ShardFor(""); !errors.Is(err, storage.ErrEmptyKey) {
		t.Errorf("ShardFor(\"\") returned %v, want ErrEmptyKey", err)
	}
}

func indexOf(hay []string, needle string) int {
	for i, s := range hay {
		if s == needle {
			return i
		}
	}
	return -1
}
