package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// What the gutter is for, and what it must refuse to do.
//
// A circuit breaker protects LATENCY: it stops a dying cache node holding requests open. It does
// nothing for the ORIGIN, which receives that node's entire share of the keyspace the moment the
// node stops answering — with three nodes, a third of all reads arriving at the database at once,
// and the database has no breaker.
//
// The gutter catches them. What it must not do is quietly weaken a guarantee: its entries are never
// invalidated, so every rule that decides whether an entry is fresh enough has to apply to them
// unchanged, and a read it serves has to say what it is.
//
// White-box, because the interesting states are "the home node is not answering" and "it answered a
// miss", and those are indistinguishable from outside.

// stubCache is a cache that can be told to fail.
type stubCache struct {
	entries map[string]cache.Entry
	fail    error

	fills      int
	tombstones int
}

func newStubCache() *stubCache { return &stubCache{entries: map[string]cache.Entry{}} }

func (s *stubCache) Get(_ context.Context, key string) (cache.Entry, bool, error) {
	if s.fail != nil {
		return cache.Entry{}, false, s.fail
	}
	e, ok := s.entries[key]
	return e, ok, nil
}

func (s *stubCache) Fill(_ context.Context, key string, e cache.Entry) (bool, error) {
	if s.fail != nil {
		return false, s.fail
	}
	s.fills++
	s.entries[key] = e
	return true, nil
}

func (s *stubCache) Tombstone(_ context.Context, key string, _ uint64) (bool, error) {
	if s.fail != nil {
		return false, s.fail
	}
	s.tombstones++
	delete(s.entries, key)
	return true, nil
}

func (s *stubCache) GetOrLease(_ context.Context, key string) (cache.LeaseResult, error) {
	if s.fail != nil {
		return cache.LeaseResult{}, s.fail
	}
	if e, ok := s.entries[key]; ok {
		return cache.LeaseResult{Outcome: cache.LeaseHit, Entry: e}, nil
	}
	return cache.LeaseResult{Outcome: cache.LeaseGranted, Token: "tok"}, nil
}

func (s *stubCache) FillWithLease(ctx context.Context, key string, e cache.Entry, _ string) (bool, error) {
	return s.Fill(ctx, key, e)
}

// gutterEngine builds an engine with a primary and a standby, and no storage: every test here is
// about what happens before the origin is reached.
func gutterEngine(t *testing.T, primary, gutter Cache) *Engine {
	t.Helper()

	e := &Engine{
		cache:     primary,
		gutter:    gutter,
		gutterTTL: 30 * time.Second,
		now:       time.Now,
		leases:    NewWaitPolicy(0, 0, 0),
	}
	e.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	return e
}

func anEntry(fillVersion uint64) cache.Entry {
	return cache.Entry{RowVersion: fillVersion, FillVersion: fillVersion, Row: []byte("row")}
}

func eventual() consistency.Requirement {
	return consistency.Requirement{Level: consistency.Eventual}
}

// TestTheGutterIsConsultedOnlyWhenTheHomeNodeIsNotAnswering.
//
// A miss means the answer is not cached, and the origin is where it comes from. Consulting the
// gutter then would serve an entry the primary had already superseded — the primary is the pool
// invalidations go to, so a value living only in the gutter is a value nothing has invalidated.
func TestTheGutterIsConsultedOnlyWhenTheHomeNodeIsNotAnswering(t *testing.T) {
	t.Parallel()

	primary, gutter := newStubCache(), newStubCache()
	gutter.entries["k"] = anEntry(100)
	e := gutterEngine(t, primary, gutter)

	// The primary answers, and answers a miss.
	_, served, _, fromGutter := e.fromCacheOrLease(context.Background(), eventual(), "k", "shard0", token())
	if served {
		t.Error("a primary MISS was served from the gutter; the primary is the pool invalidations " +
			"reach, so an entry living only in the gutter is one nothing has invalidated")
	}
	if fromGutter {
		t.Error("the read was attributed to the gutter without the home node having failed")
	}
}

// TestAnUnansweringHomeNodeIsServedFromTheGutter is the whole point.
func TestAnUnansweringHomeNodeIsServedFromTheGutter(t *testing.T) {
	t.Parallel()

	primary, gutter := newStubCache(), newStubCache()
	primary.fail = errors.New("connection refused")
	gutter.entries["k"] = anEntry(100)
	e := gutterEngine(t, primary, gutter)

	entry, served, _, fromGutter := e.fromCacheOrLease(context.Background(), eventual(), "k", "shard0", token())
	if !served {
		t.Fatal("the home node was down, the gutter held the key, and the read still went to the origin")
	}
	if !fromGutter {
		t.Error("the read was served from the gutter and not reported as such; the caller cannot " +
			"act on a weakened answer it was not told about")
	}
	if entry.FillVersion != 100 {
		t.Errorf("fill version = %d, want the gutter entry's 100", entry.FillVersion)
	}
}

// TestAGutterMissStillReachesTheOrigin: a standby pool is not a second source of truth.
func TestAGutterMissStillReachesTheOrigin(t *testing.T) {
	t.Parallel()

	primary, gutter := newStubCache(), newStubCache()
	primary.fail = errors.New("connection refused")
	e := gutterEngine(t, primary, gutter)

	if _, served, _, _ := e.fromCacheOrLease(context.Background(), eventual(), "k", "shard0", token()); served {
		t.Error("an empty gutter served a read")
	}
}

// TestAFailingGutterIsNotAnError. The gutter exists to reduce origin load, not to become another
// thing that can fail a request: with both pools down the read goes to the database, which is where
// it was going before the gutter existed.
func TestAFailingGutterIsNotAnError(t *testing.T) {
	t.Parallel()

	primary, gutter := newStubCache(), newStubCache()
	primary.fail = errors.New("connection refused")
	gutter.fail = errors.New("connection refused")
	e := gutterEngine(t, primary, gutter)

	if _, served, _, _ := e.fromCacheOrLease(context.Background(), eventual(), "k", "shard0", token()); served {
		t.Error("a failing gutter served a read")
	}
}

// TestTheSessionWatermarkAppliesToAGutterEntry is the guarantee that survives a pool nothing
// invalidates, and the reason the gutter is safe at SESSION at all.
//
// Read-own-writes is carried by the WATERMARK, not by invalidation: a session that wrote at version
// 200 rejects any entry filled before 200, wherever that entry lives. The gutter changes which pool
// answers, never which entries are acceptable.
func TestTheSessionWatermarkAppliesToAGutterEntry(t *testing.T) {
	t.Parallel()

	primary, gutter := newStubCache(), newStubCache()
	primary.fail = errors.New("connection refused")
	gutter.entries["k"] = anEntry(100)
	e := gutterEngine(t, primary, gutter)

	// A session that has already observed version 200 on this shard.
	tok := token()
	tok.Advance("shard0", 200)

	req := consistency.Requirement{Level: consistency.Session}
	_, served, _, _ := e.fromCacheOrLease(context.Background(), req, "k", "shard0", tok)
	if served {
		t.Error("a gutter entry older than the session's own write was served; read-own-writes is " +
			"carried by the watermark, and the gutter must not be a way around it")
	}

	// The same entry IS acceptable to a session that has seen nothing newer.
	if _, served, _, _ := e.fromCacheOrLease(context.Background(), req, "k", "shard0", token()); !served {
		t.Error("a gutter entry the session had no reason to reject was refused anyway")
	}
}

// TestAStaleGutterEntryTakesNoLease. The lease is per-key state on the home node, and the home node
// is the one that is not answering.
func TestAStaleGutterEntryTakesNoLease(t *testing.T) {
	t.Parallel()

	primary, gutter := newStubCache(), newStubCache()
	primary.fail = errors.New("connection refused")
	gutter.entries["k"] = anEntry(100)
	e := gutterEngine(t, primary, gutter)

	tok := token()
	tok.Advance("shard0", 200)

	_, served, lease, fromGutter := e.fromCacheOrLease(
		context.Background(), consistency.Requirement{Level: consistency.Session}, "k", "shard0", tok)
	if served {
		t.Fatal("a stale gutter entry was served")
	}
	if lease != "" {
		t.Error("a lease was taken on a node that is not answering")
	}
	if !fromGutter {
		t.Error("the refill was not directed at the gutter, so it would go to the node that is down")
	}
}

// TestTheGutterIsFilledWhenTheHomeNodeIsDown. Without this the gutter never catches anything: the
// first reader of each key would reach the origin, and so would the second, and the third.
func TestTheGutterIsFilledWhenTheHomeNodeIsDown(t *testing.T) {
	t.Parallel()

	primary, gutter := newStubCache(), newStubCache()
	primary.fail = errors.New("connection refused")
	e := gutterEngine(t, primary, gutter)

	e.fillGutter(context.Background(), "k", anEntry(100))
	if gutter.fills != 1 {
		t.Errorf("the gutter took %d fills, want 1", gutter.fills)
	}
	if primary.fills != 0 {
		t.Errorf("the primary took %d fills while it was down", primary.fills)
	}
}

// TestTheGutterIsTombstonedOnlyWhenTheHomeNodeRefused.
//
// Invalidating the gutter on every write would double the invalidation traffic for a pool that is
// almost never read. Doing it when the home node REFUSED the tombstone targets exactly the
// condition under which the gutter can be holding a superseded value.
func TestTheGutterIsTombstonedOnlyWhenTheHomeNodeRefused(t *testing.T) {
	t.Parallel()

	primary, gutter := newStubCache(), newStubCache()
	gutter.entries["k"] = anEntry(100)
	e := gutterEngine(t, primary, gutter)
	e.syncInvalidation = true

	// The home node takes it: the gutter is left alone.
	e.invalidate(context.Background(), "k", storage.Version(200))
	if gutter.tombstones != 0 {
		t.Errorf("the gutter was tombstoned %d times while the home node was answering", gutter.tombstones)
	}

	// The home node refuses: the gutter is the only place a superseded value can be.
	primary.fail = errors.New("connection refused")
	e.invalidate(context.Background(), "k", storage.Version(300))
	if gutter.tombstones != 1 {
		t.Errorf("the gutter took %d tombstones after the home node refused, want 1", gutter.tombstones)
	}
}

// TestWithoutAGutterNothingChanges. The pool is optional, and an engine without one has to behave
// exactly as it did before it existed.
func TestWithoutAGutterNothingChanges(t *testing.T) {
	t.Parallel()

	primary := newStubCache()
	primary.fail = errors.New("connection refused")
	e := gutterEngine(t, primary, nil)

	_, served, lease, fromGutter := e.fromCacheOrLease(context.Background(), eventual(), "k", "shard0", token())
	if served || lease != "" || fromGutter {
		t.Errorf("an engine with no gutter behaved differently: served=%t lease=%q fromGutter=%t",
			served, lease, fromGutter)
	}

	e.syncInvalidation = true
	e.invalidate(context.Background(), "k", storage.Version(1)) // must not panic on a nil gutter
	e.fillGutter(context.Background(), "k", anEntry(1))
}

func token() *consistency.Token {
	return consistency.TokenFromProto(nil, consistency.DefaultMaxSessionShards)
}
