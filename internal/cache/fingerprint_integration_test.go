//go:build integration

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
)

// clientWith returns a client that believes rows have the given shape.
//
// It shares the package's container, and deliberately does NOT flush: these tests are about two
// clients disagreeing about one cache, so each needs the other's writes to survive.
func clientWith(ctx context.Context, t *testing.T, fingerprint string) *cache.Client {
	t.Helper()

	once.Do(func() { addr, valkey, startErr = startValkey(ctx) })
	if startErr != nil {
		t.Fatalf("start valkey: %v", startErr)
	}

	c, err := cache.New(ctx, cache.Options{
		Addresses:   []string{addr},
		TTL:         time.Hour,
		LeaseTTL:    time.Minute,
		Fingerprint: fingerprint,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// An entry written for one row shape must not be served to a reader expecting another.
//
// This is what makes a schema change safe with nothing to flush: the old entries simply stop being
// served. Decoding them instead would hand a caller a row whose columns are not the ones it asked
// for, which is the worst possible answer — confidently wrong.
func TestAnEntryWrittenForAnotherShapeReadsAsAMiss(t *testing.T) {
	ctx := context.Background()
	const key = "entities:shape-miss"

	old := clientWith(ctx, t, "shape-aaaa")
	if _, err := old.Fill(ctx, key, cache.Entry{RowVersion: 10, FillVersion: 10, Row: []byte("old-shape")}); err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if _, hit, err := old.Get(ctx, key); err != nil || !hit {
		t.Fatalf("the writer cannot read its own entry: hit=%v err=%v", hit, err)
	}

	fresh := clientWith(ctx, t, "shape-bbbb")
	_, hit, err := fresh.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if hit {
		t.Error("an entry written for a different row shape was served")
	}
}

// And the miss must take the LEASE path.
//
// A schema change invalidates every entry at once. If each reader only discovered that after being
// told "hit", every one of them would go to the origin with no lease protecting it — a fleet-wide
// stampede at the exact moment a deployment is already in motion. The lease is the difference
// between a deploy that costs one origin read per key and one that costs all of them.
func TestAShapeMismatchTakesTheLeasePath(t *testing.T) {
	ctx := context.Background()
	const key = "entities:shape-lease"

	old := clientWith(ctx, t, "shape-cccc")
	if _, err := old.Fill(ctx, key, cache.Entry{RowVersion: 10, FillVersion: 10, Row: []byte("old-shape")}); err != nil {
		t.Fatalf("Fill: %v", err)
	}

	fresh := clientWith(ctx, t, "shape-dddd")
	res, err := fresh.GetOrLease(ctx, key)
	if err != nil {
		t.Fatalf("GetOrLease: %v", err)
	}
	if res.Outcome != cache.LeaseGranted {
		t.Fatalf("outcome = %v, want LeaseGranted: a reader meeting a foreign shape must be "+
			"admitted to fill, not sent to the origin unprotected", res.Outcome)
	}
}

// The rolling-deploy property, and the reason the tombstone carries no fingerprint.
//
// Mid-deploy, two engines run different shapes against one cache. An invalidation is a statement
// about a ROW, not about a schema, so an old engine's tombstone must still block a new engine's
// fill — otherwise the two invalidate past each other and a write made during the deploy is lost
// by whichever engine did not see it.
func TestATombstoneCrossesAShapeChange(t *testing.T) {
	ctx := context.Background()
	const key = "entities:shape-tombstone"

	old := clientWith(ctx, t, "shape-eeee")
	fresh := clientWith(ctx, t, "shape-ffff")

	// The old engine invalidates at version 50.
	applied, err := old.Tombstone(ctx, key, 50)
	if err != nil || !applied {
		t.Fatalf("Tombstone: applied=%v err=%v", applied, err)
	}

	// The new engine's in-flight fill carries an OLDER version and must lose, despite the
	// tombstone having been written by an engine with a different shape.
	applied, err = fresh.Fill(ctx, key, cache.Entry{RowVersion: 40, FillVersion: 40, Row: []byte("stale")})
	if err != nil {
		t.Fatalf("Fill: %v", err)
	}
	if applied {
		t.Error("a fill older than a foreign-shape tombstone was applied: the two engines " +
			"invalidated past each other and a write made during the deploy is now lost")
	}

	// A newer fill still wins, so the key is not stuck.
	applied, err = fresh.Fill(ctx, key, cache.Entry{RowVersion: 60, FillVersion: 60, Row: []byte("fresh")})
	if err != nil || !applied {
		t.Fatalf("a newer fill was rejected: applied=%v err=%v", applied, err)
	}
}
