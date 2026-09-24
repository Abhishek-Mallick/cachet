//go:build e2e

// Package verifier_test is the conformance suite for Sextant itself.
//
// Everything else in this repository tests whether Cachet is consistent. This tests whether the
// thing that MEASURES consistency can be believed — against a plain Redis and a plain MySQL, with
// no Cachet anywhere, which is the deployment Sextant claims to work on.
//
// The shape of it is the argument. Four writers run against the same table and the same cache:
// three are deliberately broken in ways real systems are broken, and one is correct. A verifier
// that reports violations for the broken ones and none for the correct one over a sustained run is
// a verifier; one that reports violations for everything is a noise generator, and one that reports
// none for anything is worse, because it comes with a reassuring number attached.
package verifier_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
	"github.com/Abhishek-Mallick/cachet/pkg/sextant"
)

const (
	originDSN = "root:cachet@tcp(127.0.0.1:3316)/cachet"
	cacheAddr = "127.0.0.1:6379"

	// A short bound, because this suite has to finish. It is still the real arithmetic: an entry
	// must be behind for LONGER than this before anything is reported, which is what separates a
	// benign in-flight race from a violation.
	writeBudget  = 100 * time.Millisecond
	cdcLagBound  = 200 * time.Millisecond
	maxClockSkew = 50 * time.Millisecond

	// The table and key layout are the APPLICATION's, not Cachet's. That is the point: nothing
	// here carries an HLC, a fingerprint, or any other thing Cachet puts in an entry.
	table       = "verifier_rows"
	keyTemplate = "vtest:{key}"
)

const schemaDDL = `
CREATE TABLE IF NOT EXISTS verifier_rows (
  id          VARCHAR(64)      NOT NULL,
  payload     VARCHAR(255)     NOT NULL,
  row_version BIGINT UNSIGNED  NOT NULL,
  PRIMARY KEY (id)
) ENGINE=ROCKSDB`

// ─── the system under test: somebody else's cache, in front of somebody else's database ─────

// writer is an application keeping a cache beside a database. Each implementation is a different
// answer to the hardest question in caching, and three of them are wrong.
type writer interface {
	// Name is what appears in the report.
	Name() string

	// Broken reports whether this writer is expected to produce violations.
	Broken() bool

	// Why explains the failure mode in one sentence, for the published table.
	Why() string

	// Write commits a new value for a key and does whatever this writer does about the cache.
	Write(ctx context.Context, t *testing.T, key, payload string, version uint64)
}

type app struct {
	db    *sql.DB
	redis redis.UniversalClient
}

func (a *app) commit(ctx context.Context, t *testing.T, key, payload string, version uint64) {
	t.Helper()

	_, err := a.db.ExecContext(ctx,
		"INSERT INTO verifier_rows (id, payload, row_version) VALUES (?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE payload = VALUES(payload), row_version = VALUES(row_version)",
		key, payload, version)
	if err != nil {
		t.Fatalf("commit %s: %v", key, err)
	}
}

func (a *app) fill(ctx context.Context, t *testing.T, key, payload string, version uint64) {
	t.Helper()

	body, err := json.Marshal(map[string]any{"payload": payload, "row_version": version})
	if err != nil {
		t.Fatalf("encode %s: %v", key, err)
	}
	if err := a.redis.Set(ctx, cacheKey(key), body, time.Hour).Err(); err != nil {
		t.Fatalf("fill %s: %v", key, err)
	}
}

// correctWriter commits, then invalidates. The ordering is the whole thing: after the commit, so a
// concurrent reader cannot refill the pre-write value; before the acknowledgement, so the caller's
// next read cannot see the entry it just superseded.
type correctWriter struct{ *app }

func (correctWriter) Name() string { return "invalidate-after-commit" }
func (correctWriter) Broken() bool { return false }

func (correctWriter) Why() string {
	return "commits, then deletes the entry — the correct ordering"
}

func (w correctWriter) Write(ctx context.Context, t *testing.T, key, payload string, version uint64) {
	w.commit(ctx, t, key, payload, version)
	if err := w.redis.Del(ctx, cacheKey(key)).Err(); err != nil {
		t.Fatalf("invalidate %s: %v", key, err)
	}
	// The refill the next read would do. Without it this writer leaves no entry at all, and a
	// verifier has nothing to be wrong about — which would make "zero violations" a statement about
	// an empty cache rather than about a correct one.
	w.fill(ctx, t, key, payload, version)
}

// ttlOnlyWriter is the most common cache in production: fill on read, expire on a timer, never
// invalidate. It is not a bug in anyone's code — it is a design that trades correctness for
// simplicity, usually without anybody writing down what was traded.
type ttlOnlyWriter struct{ *app }

func (ttlOnlyWriter) Name() string { return "ttl-only" }
func (ttlOnlyWriter) Broken() bool { return true }
func (ttlOnlyWriter) Why() string {
	return "commits and leaves the entry alone; it goes stale until the TTL expires"
}

func (w ttlOnlyWriter) Write(ctx context.Context, t *testing.T, key, payload string, version uint64) {
	w.commit(ctx, t, key, payload, version)
}

// deleteBeforeCommitWriter invalidates first and commits second, which looks careful and is the
// classic race: between the delete and the commit, any read refills the entry from the PRE-write
// database state, and that entry then outlives the write.
type deleteBeforeCommitWriter struct{ *app }

func (deleteBeforeCommitWriter) Name() string { return "delete-before-commit" }
func (deleteBeforeCommitWriter) Broken() bool { return true }
func (deleteBeforeCommitWriter) Why() string {
	return "deletes then commits; a read in between refills the pre-write value, which then survives"
}

func (w deleteBeforeCommitWriter) Write(ctx context.Context, t *testing.T, key, payload string, version uint64) {
	// The refill this race loses to, made deterministic: a concurrent reader would do exactly this
	// between the two statements, and a test that waited for one to happen by luck would be
	// flaky about the thing it is asserting.
	old, oldVersion := w.current(ctx, t, key)
	if err := w.redis.Del(ctx, cacheKey(key)).Err(); err != nil {
		t.Fatalf("invalidate %s: %v", key, err)
	}
	w.fill(ctx, t, key, old, oldVersion)
	w.commit(ctx, t, key, payload, version)
}

func (a *app) current(ctx context.Context, t *testing.T, key string) (string, uint64) {
	t.Helper()

	var payload string
	var version uint64
	err := a.db.QueryRowContext(ctx,
		"SELECT payload, row_version FROM verifier_rows WHERE id = ?", key).Scan(&payload, &version)
	switch {
	case err == sql.ErrNoRows:
		return "", 0
	case err != nil:
		t.Fatalf("read %s: %v", key, err)
	}
	return payload, version
}

// racyWriteThroughWriter writes the cache and the database in the same breath, with no ordering
// between them. Under concurrency two writers interleave and the cache ends up holding the older
// one — a write-through cache that is right almost always, which is the hardest kind of wrong to
// find without a verifier.
type racyWriteThroughWriter struct{ *app }

func (racyWriteThroughWriter) Name() string { return "racy-write-through" }
func (racyWriteThroughWriter) Broken() bool { return true }
func (racyWriteThroughWriter) Why() string {
	return "fills the cache with a value older than the one it commits"
}

func (w racyWriteThroughWriter) Write(ctx context.Context, t *testing.T, key, payload string, version uint64) {
	// The interleaving, made deterministic for the same reason as above: this is what two
	// concurrent write-throughs produce when the slower one's cache write lands last.
	w.commit(ctx, t, key, payload, version)
	if version > 1 {
		w.fill(ctx, t, key, payload+"-stale", version-1)
	}
}

// ─── the suite ──────────────────────────────────────────────────────────────────

func cacheKey(key string) string { return strings.ReplaceAll(keyTemplate, "{key}", key) }

func newApp(t *testing.T) *app {
	t.Helper()

	db, err := sql.Open("mysql", originDSN)
	if err != nil {
		t.Fatalf("open origin: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Skipf("no database at %s: %v", originDSN, err)
	}
	if _, err := db.ExecContext(context.Background(), schemaDDL); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{cacheAddr}})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no cache at %s: %v", cacheAddr, err)
	}
	return &app{db: db, redis: rdb}
}

func writers(a *app) []writer {
	return []writer{
		correctWriter{a},
		ttlOnlyWriter{a},
		deleteBeforeCommitWriter{a},
		racyWriteThroughWriter{a},
	}
}

// buildVerifier points Sextant at the plain Redis and the plain MySQL, at the version tier.
func buildVerifier(ctx context.Context, t *testing.T, keys sextant.KeySource) (*sextant.Verifier, *sextant.SLO, func()) {
	t.Helper()

	origin, err := sextant.NewSQLOrigin(ctx, sextant.SQLOriginOptions{
		DSNs: []string{originDSN}, Table: table, KeyColumn: "id",
		VersionColumn: "row_version", ValueColumns: []string{"payload"},
	})
	if err != nil {
		t.Fatalf("NewSQLOrigin: %v", err)
	}
	cache, err := sextant.NewRedisCache(ctx, sextant.RedisCacheOptions{
		Addrs: []string{cacheAddr}, KeyTemplate: keyTemplate,
		Codec: sextant.CodecJSON, VersionField: "row_version",
	})
	if err != nil {
		_ = origin.Close()
		t.Fatalf("NewRedisCache: %v", err)
	}

	slo := sextant.NewSLO(time.Minute)
	v, err := sextant.NewVerifier(sextant.VerifierOptions{
		Cache: cache, Origin: origin, Keys: keys, Tier: sextant.TierVersion,
		Bound:    sextant.NewPropagationBound(writeBudget, cdcLagBound, maxClockSkew),
		SLO:      slo,
		Interval: 20 * time.Millisecond,
		Batch:    64,
	})
	if err != nil {
		_ = cache.Close()
		_ = origin.Close()
		t.Fatalf("NewVerifier: %v", err)
	}
	return v, slo, func() {
		_ = cache.Close()
		_ = origin.Close()
	}
}

// TestTheVerifierFindsEveryBrokenWriterAndAccusesTheCorrectOneOfNothing is the whole suite in one
// test, because the two halves only mean something together.
//
// A verifier that reports violations everywhere is a noise generator and will be turned off within
// a week. One that reports none is worse: it arrives with a reassuring number attached, and the
// first person to trust it is the last person who will.
func TestTheVerifierFindsEveryBrokenWriterAndAccusesTheCorrectOneOfNothing(t *testing.T) {
	ctx := context.Background()
	a := newApp(t)

	for _, w := range writers(a) {
		t.Run(w.Name(), func(t *testing.T) {
			keys := sextant.NewRecentKeys(64)
			v, _, closeAll := buildVerifier(ctx, t, keys)
			defer closeAll()

			// Twenty keys, each written twice: once to establish an entry, once to change the row
			// underneath it. What each writer does about the cache in between is the difference.
			prefix := fmt.Sprintf("%s-%d", w.Name(), time.Now().UnixNano())
			for i := range 20 {
				key := fmt.Sprintf("%s-%d", prefix, i)
				w.Write(ctx, t, key, "v1", 1)
				a.fill(ctx, t, key, "v1", 1)
				w.Write(ctx, t, key, "v2", 2)
				keys.Touch(key)
			}

			// Sampled until past the propagation bound, because a violation is a DURATION: an entry
			// behind for less than the bound is an invalidation in flight, and the model never
			// promised otherwise.
			var stats sextant.Stats
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				round := v.RunOnce(ctx)
				stats.Checked += round.Checked
				stats.Violations += round.Violations
				stats.Errors += round.Errors
				if round.Errors > 0 {
					t.Fatalf("the verifier errored while sampling: %+v", round)
				}
				if !w.Broken() && stats.Checked > 200 {
					break
				}
				if w.Broken() && stats.Violations > 0 {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}

			if stats.Checked == 0 {
				t.Fatal("nothing was checked; the assertion below would prove nothing")
			}
			switch {
			case w.Broken() && stats.Violations == 0:
				t.Errorf("%s is broken — %s — and the verifier found nothing in %d checks",
					w.Name(), w.Why(), stats.Checked)
			case !w.Broken() && stats.Violations > 0:
				t.Errorf("%s is correct — %s — and the verifier accused it %d times in %d checks",
					w.Name(), w.Why(), stats.Violations, stats.Checked)
			}
			t.Logf("%s: %d checked, %d violations", w.Name(), stats.Checked, stats.Violations)
		})
	}
}

// TestTheFalsePositiveRateUnderAWriteBurst publishes a number rather than asserting a vibe.
//
// The interesting failure is not a verifier that is wrong about a broken cache; it is one that is
// wrong about a CORRECT cache under load, because that is the condition every real deployment is in
// and the condition under which somebody decides the tool is noise. The rate is measured against
// the correct writer running as fast as it can, and written to FALSE-POSITIVES.md so it is a
// published figure rather than a claim.
func TestTheFalsePositiveRateUnderAWriteBurst(t *testing.T) {
	ctx := context.Background()
	a := newApp(t)
	w := correctWriter{a}

	keys := sextant.NewRecentKeys(512)
	v, slo, closeAll := buildVerifier(ctx, t, keys)
	defer closeAll()

	prefix := fmt.Sprintf("burst-%d", time.Now().UnixNano())
	for i := range 200 {
		key := fmt.Sprintf("%s-%d", prefix, i)
		w.Write(ctx, t, key, "v0", 1)
		a.fill(ctx, t, key, "v0", 1)
		keys.Touch(key)
	}

	// Writers hammering the same keys the verifier is sampling. Every one of them is correct, so
	// every violation reported here is a false positive by construction.
	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		version  atomic.Uint64
		burstErr atomic.Pointer[error]
	)
	version.Store(1)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := version.Add(1)
				key := fmt.Sprintf("%s-%d", prefix, n%200)
				func() {
					defer func() {
						if r := recover(); r != nil {
							err := fmt.Errorf("burst writer: %v", r)
							burstErr.Store(&err)
						}
					}()
					w.Write(ctx, t, key, fmt.Sprintf("v%d", n), n)
				}()
			}
		}()
	}

	var stats sextant.Stats
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		round := v.RunOnce(ctx)
		stats.Checked += round.Checked
		stats.Violations += round.Violations
		stats.Errors += round.Errors
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if err := burstErr.Load(); err != nil {
		t.Fatalf("the burst writers failed: %v", *err)
	}
	if stats.Checked < 500 {
		t.Fatalf("only %d observations; too few to state a rate", stats.Checked)
	}

	rate := float64(stats.Violations) / float64(stats.Checked)
	report := sextant.Report{}
	if slo != nil {
		report = slo.Report(consistency.Eventual, time.Now())
	}
	t.Logf("false positives: %d in %d observations (%.4f%%), errors %d, SLO nines %.2f",
		stats.Violations, stats.Checked, rate*100, stats.Errors, report.Nines)

	writeFalsePositiveReport(t, stats, rate)

	// The threshold is deliberately generous, and it is a threshold rather than zero because this
	// runs against a real database and a real Redis on a machine doing other things. What it
	// forbids is the failure that matters: a verifier that reports a few percent of a CORRECT
	// system as broken is one nobody will keep running.
	if rate > 0.01 {
		t.Errorf("false-positive rate %.4f%% over %d observations of a correct writer; "+
			"a verifier this noisy gets turned off", rate*100, stats.Checked)
	}
}

// writeFalsePositiveReport publishes the measured rate.
//
// A file rather than a log line, because the claim "Sextant does not cry wolf" is only worth
// anything if the number behind it is visible without re-running the suite.
func writeFalsePositiveReport(t *testing.T, stats sextant.Stats, rate float64) {
	t.Helper()

	body := fmt.Sprintf(`# Sextant — measured false-positive rate

Generated by `+"`test/verifier`"+`. Do not edit by hand.

A verifier that reports violations against a CORRECT cache is one nobody keeps running, so the rate
is measured rather than asserted. Four writers commit and then invalidate — the correct ordering —
as fast as they can against the same keys the verifier is sampling. Every violation reported under
that load is a false positive by construction.

| | |
|---|---|
| Observations | %d |
| Violations | %d |
| False-positive rate | **%.4f%%** |
| Sampling errors | %d |
| Propagation bound | %s |
| Tier | version |

The propagation bound is what makes the rate low, and it is not a fudge factor: an entry behind for
less than the bound is an invalidation in flight, which the model never promised would not happen.
An entry behind for longer is something that was dropped.
`, stats.Checked, stats.Violations, rate*100, stats.Errors, writeBudget+cdcLagBound+maxClockSkew)

	// The repository root, beside FAULTS.md, because docs/ is not published and a measured claim
	// nobody can read is a claim.
	path := "../../FALSE-POSITIVES.md"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Logf("could not write %s: %v", path, err)
	}
}
