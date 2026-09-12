//go:build e2e

// Package conformance executes the consistency matrix in CONSISTENCY.md §8.
//
// This is THE gate. Everything else Cachet claims rests on it: the product's entire pitch is that it
// can state a guarantee and prove it, and a guarantee nobody has watched fail is a guarantee nobody
// has tested. So this suite does two things, not one:
//
//	TestConformanceMatrix          — every ✅ cell must hold, on the shipped configuration
//	TestTheSuiteDetectsAViolation  — the same cells must FAIL on a Phase 1 naive cache
//
// The second is the important one. Without it, a suite that asserted nothing would pass exactly as
// convincingly as one that worked (build plan §12: "a consistency test that has never failed is
// proving nothing").
package conformance_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/test/harness"
)

// outcome is what CONSISTENCY.md §8 promises for one cell.
type outcome int

const (
	// guaranteed is a ✅: the property must hold, and a failure is a bug.
	guaranteed outcome = iota
	// notGuaranteed is a ⬜: the model explicitly does not promise this. The cell is still
	// EXECUTED, and what happened is recorded — but it is never asserted, because asserting it
	// would either invent a promise the model does not make or forbid the system from being
	// incidentally better than its contract.
	notGuaranteed
	// never is a ❌: the property must NOT hold at any level. Cross-key snapshots are the only one,
	// and a cell that quietly started holding would be a promise nobody decided to make.
	never
	// notApplicable is n/a.
	notApplicable
)

func (o outcome) String() string {
	switch o {
	case guaranteed:
		return "guaranteed"
	case notGuaranteed:
		return "not guaranteed"
	case never:
		return "never"
	default:
		return "n/a"
	}
}

// levels in the order CONSISTENCY.md §8 lists them.
var levels = []struct {
	name  string
	level cachetv1.ConsistencyLevel
	// bound is the staleness bound, required for BOUNDED and rejected for every other level.
	bound time.Duration
}{
	{"STRONG", cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG, 0},
	{"SESSION", cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION, 0},
	{"BOUNDED", cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_BOUNDED, 2 * time.Second},
	{"EVENTUAL", cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL, 0},
}

// cell is one operation, with the outcome expected at each level.
type cell struct {
	op string
	// run performs the scenario and reports whether the property held.
	run func(t *testing.T, env *env, lv levelSpec) bool
	// expect is indexed by the levels slice.
	expect [4]outcome
}

type levelSpec struct {
	level cachetv1.ConsistencyLevel
	bound time.Duration
}

// matrix is CONSISTENCY.md §8, executable. The table below and the table in that document must
// agree; if they drift, one of them is lying to a reader who trusted it.
//
//	| Operation                             | STRONG | SESSION | BOUNDED | EVENTUAL |
var matrix = []cell{
	{
		op:     "read own point write",
		run:    readOwnPointWrite,
		expect: [4]outcome{guaranteed, guaranteed, guaranteed, notGuaranteed},
	},
	{
		op:     "read own insert over a negative entry",
		run:    readOwnInsertOverNegative,
		expect: [4]outcome{guaranteed, guaranteed, guaranteed, notGuaranteed},
	},
	{
		op:     "read own delete",
		run:    readOwnDelete,
		expect: [4]outcome{guaranteed, guaranteed, guaranteed, notGuaranteed},
	},
	{
		op:     "read another session's write, immediately",
		run:    readAnotherSessionImmediately,
		expect: [4]outcome{guaranteed, notGuaranteed, notGuaranteed, notGuaranteed},
	},
	{
		op:     "read another session's write, after P",
		run:    readAnotherSessionAfterP,
		expect: [4]outcome{guaranteed, guaranteed, guaranteed, guaranteed},
	},
	{
		op:     "monotonic reads under concurrent writers",
		run:    monotonicReads,
		expect: [4]outcome{guaranteed, guaranteed, guaranteed, notGuaranteed},
	},
	{
		op:     "staleness bounded by t",
		run:    stalenessBounded,
		expect: [4]outcome{notApplicable, notGuaranteed, guaranteed, notGuaranteed},
	},
	{
		op:     "survives a cache flush",
		run:    survivesCacheFlush,
		expect: [4]outcome{guaranteed, guaranteed, guaranteed, guaranteed},
	},
	{
		op:     "survives engine failover",
		run:    survivesEngineFailover,
		expect: [4]outcome{guaranteed, guaranteed, guaranteed, guaranteed},
	},
	{
		op:     "cross-key snapshot",
		run:    crossKeySnapshot,
		expect: [4]outcome{never, never, never, never},
	},
}

// TestConformanceMatrix executes every cell on the shipped configuration.
func TestConformanceMatrix(t *testing.T) {
	env := newEnv(t, config{synchronousInvalidation: true})

	for _, c := range matrix {
		for i, lv := range levels {
			want := c.expect[i]
			if want == notApplicable {
				continue
			}

			t.Run(fmt.Sprintf("%s/%s", lv.name, c.op), func(t *testing.T) {
				held := c.run(t, env, levelSpec{level: lv.level, bound: lv.bound})

				switch want {
				case guaranteed:
					if !held {
						t.Errorf("%s at %s: CONSISTENCY.md §8 promises this and it did not hold", c.op, lv.name)
					}
				case never:
					if held {
						t.Errorf("%s at %s: this must never hold, but it did — a promise nobody decided to make",
							c.op, lv.name)
					}
				case notGuaranteed:
					// Executed and recorded, never asserted. Logging it is the point: it is how a
					// reader learns whether a ⬜ is merely unpromised or actually absent.
					t.Logf("%s at %s: not guaranteed; observed held=%v", c.op, lv.name, held)
				}
			})
		}
	}
}

// TestTheSuiteDetectsAViolation runs the invalidation-dependent cells against a Phase 1 naive cache
// — TTL only, no invalidation of any kind — and requires them to FAIL.
//
// This is the test that makes the rest of the suite mean something. The build plan's middle
// checkpoint asks for the conformance suite to be *observed failing first*; encoding that as a
// permanent test is stronger than observing it once by hand, because it keeps being true.
//
// Only the cells that genuinely depend on invalidation are listed. Read-own-writes is deliberately
// NOT among them: Phase 1 established that it holds with no invalidation at all, carried entirely by
// the session watermark, and claiming it should break here would be inventing a failure to look
// thorough.
func TestTheSuiteDetectsAViolation(t *testing.T) {
	env := newEnv(t, config{synchronousInvalidation: false})

	// With no invalidation, another session's write never becomes visible through a cached entry:
	// the entry simply sits there until its TTL. STRONG is excluded because it bypasses the cache
	// entirely, so it cannot be affected by invalidation being off.
	for _, lv := range []struct {
		name  string
		level cachetv1.ConsistencyLevel
		bound time.Duration
	}{
		{"SESSION", cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_SESSION, 0},
		{"EVENTUAL", cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_EVENTUAL, 0},
	} {
		t.Run(lv.name+"/read another session's write, after P", func(t *testing.T) {
			held := readAnotherSessionAfterP(t, env, levelSpec{level: lv.level, bound: lv.bound})
			if held {
				t.Errorf("the naive cache passed a cell that depends on invalidation. Either the "+
					"test does not detect the violation it claims to, or invalidation is running "+
					"when it was configured off (level=%s)", lv.name)
			}
		})
	}

	t.Run("STRONG is unaffected because it bypasses the cache", func(t *testing.T) {
		// The control. If this also failed, the harness would be broken rather than the cache
		// configuration being weak, and the two must not be confusable.
		if !readAnotherSessionAfterP(t, env, levelSpec{level: cachetv1.ConsistencyLevel_CONSISTENCY_LEVEL_STRONG}) {
			t.Error("STRONG failed on the naive cache; it bypasses the cache and cannot be affected " +
				"by invalidation being off, so this points at the harness rather than the cache")
		}
	})
}

// ─── environment ────────────────────────────────────────────────────────────────

type config struct {
	synchronousInvalidation bool

	// maxAffectedKeys is the conditional-write budget. Set low by the degraded tests so degradation
	// can be observed without writing a thousand rows to provoke it.
	maxAffectedKeys int
}

type env struct {
	cluster *harness.Cluster
	client  cachetv1.CacheServiceClient
	cfg     config

	mu   sync.Mutex
	next uint64
}

func newEnv(t *testing.T, cfg config) *env {
	t.Helper()

	ctx := context.Background()
	cluster := harness.StartCachedWith(ctx, t, harness.CacheOptions{
		TTL:                     time.Hour,
		SynchronousInvalidation: cfg.synchronousInvalidation,
		MaxAffectedKeys:         cfg.maxAffectedKeys,
	}, "tcp://127.0.0.1:0")

	return &env{
		cluster: cluster,
		client:  cluster.Client(t, cluster.Addrs[0]),
		cfg:     cfg,
		// A distinct id range per run, so a rerun never inherits a previous run's rows. Sharing
		// them would make a test's outcome depend on how many times it had been run before.
		next: uint64(time.Now().UnixNano() % 1_000_000 * 1000),
	}
}

// key returns an id no other cell in this run will touch.
func (e *env) key() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.next++
	return fmt.Sprintf("entities:%d", 8_000_000_000+e.next)
}

// get reads at a level, carrying a session token.
func get(t *testing.T, e *env, key string, lv levelSpec, tok *cachetv1.SessionToken) *cachetv1.GetResponse {
	t.Helper()

	req := &cachetv1.GetRequest{Key: key, Level: lv.level, Session: tok}
	if lv.bound > 0 {
		req.StalenessBound = durationProto(lv.bound)
	}
	resp, err := e.client.Get(context.Background(), req)
	if err != nil {
		t.Fatalf("Get %s at %v: %v", key, lv.level, err)
	}
	return resp
}

func put(t *testing.T, e *env, key, payload string, tok *cachetv1.SessionToken) *cachetv1.PutResponse {
	t.Helper()

	resp, err := e.client.Put(context.Background(), &cachetv1.PutRequest{
		Key:     key,
		Record:  &cachetv1.Record{TenantId: 1, Payload: []byte(payload)},
		Session: tok,
	})
	if err != nil {
		t.Fatalf("Put %s: %v", key, err)
	}
	return resp
}
