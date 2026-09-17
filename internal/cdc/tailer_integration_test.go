//go:build integration

package cdc_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"

	"github.com/Abhishek-Mallick/cachet/internal/cdc"
)

// recordingInvalidator captures what the tailer asked to invalidate.
type recordingInvalidator struct {
	mu   sync.Mutex
	seen map[string]uint64
}

func newRecorder() *recordingInvalidator {
	return &recordingInvalidator{seen: map[string]uint64{}}
}

func (r *recordingInvalidator) Tombstone(_ context.Context, key string, version uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if version > r.seen[key] {
		r.seen[key] = version
	}
	return true, nil
}

func (r *recordingInvalidator) versionOf(key string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[key]
}

func TestMain(m *testing.M) {
	code := m.Run()
	tearDownShardContainer()
	if code == 0 {
		if err := goleak.Find(
			// testcontainers' reaper client keeps a background connection for the process lifetime.
			goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
			goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		); err != nil {
			fmt.Fprintf(os.Stderr, "goroutine leak: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

// tearDownShardContainer stops the container the package started.
//
// It builds its own context rather than taking a test's: teardown has to run after a failed or
// cancelled run, and a cancelled context would abandon the container to the reaper.
func tearDownShardContainer() {
	if shardTC == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = shardTC.Terminate(ctx)
}

// The tailer needs a REAL binlog: there is no meaningful way to fake replication, and a mocked one
// would test the mock. It gets one from a container this suite owns, NOT from the compose stack.
//
// That distinction is the tier contract in test/README.md: integration tests run against
// testcontainers and must be self-contained, while the compose stack belongs to the e2e tier.
// Pointing these at 127.0.0.1:3316 made them pass on a developer machine with `make env-up`
// running and fail in CI, where nothing had brought that stack up — a suite that only works on the
// machine that wrote it.
var (
	shardOnce sync.Once
	shardHost string
	shardDSN  string
	shardTC   testcontainers.Container
	shardErr  error
)

// shardEndpoint starts the MySQL container once for the package and returns its address and DSN.
//
// It mounts the SAME my.cnf the compose environment uses, which already enables ROW binlog with a
// FULL row image — so this also proves that configuration produces a tailable stream, rather than
// testing a convenient fiction.
func shardEndpoint(t *testing.T) (host, dsn string) {
	t.Helper()

	shardOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		shardHost, shardDSN, shardTC, shardErr = startBinlogContainer(ctx)
	})
	if shardErr != nil {
		// Fatal rather than a skip. A suite that quietly skips when its dependencies are missing
		// stops protecting anything the first time someone forgets a step, and the CI gate would go
		// green while testing nothing (test/README.md).
		t.Fatalf("start the binlog shard container: %v", shardErr)
	}
	_ = shardTC
	return shardHost, shardDSN
}

func startBinlogContainer(ctx context.Context) (host, dsn string, c testcontainers.Container, err error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", "", nil, fmt.Errorf("getwd: %w", err)
	}
	repoRoot := cwd + "/../.."

	req := testcontainers.ContainerRequest{
		Image:        "percona/percona-server:8.0",
		ExposedPorts: []string{"3306/tcp"},
		Cmd:          []string{"--defaults-extra-file=/etc/my.cnf.d/cachet.cnf", "--server-id=1"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "cachet",
			"MYSQL_DATABASE":      "cachet",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      repoRoot + "/test/env/mysql/my.myrocks.cnf",
				ContainerFilePath: "/etc/my.cnf.d/cachet.cnf",
				FileMode:          0o644,
			},
			{
				HostFilePath:      repoRoot + "/test/fixtures/schema/entities.sql",
				ContainerFilePath: "/docker-entrypoint-initdb.d/01-schema.sql",
				FileMode:          0o644,
			},
		},
		WaitingFor: wait.ForLog("port: 3306  Percona Server").WithStartupTimeout(120 * time.Second),
	}

	tc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		return "", "", nil, fmt.Errorf("start container: %w", err)
	}

	h, err := tc.Host(ctx)
	if err != nil {
		return "", "", nil, fmt.Errorf("container host: %w", err)
	}
	port, err := tc.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return "", "", nil, fmt.Errorf("mapped port: %w", err)
	}

	addr := fmt.Sprintf("%s:%s", h, port.Port())
	return addr, fmt.Sprintf("root:cachet@tcp(%s)/cachet?parseTime=true&interpolateParams=true", addr), tc, nil
}

func openDB(t *testing.T) *sql.DB {
	t.Helper()

	_, dsn := shardEndpoint(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("the shard container is not reachable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func startTailer(ctx context.Context, t *testing.T, inv cdc.Invalidator, serverID uint32, cp cdc.Checkpoint) *cdc.Tailer {
	t.Helper()

	addr, _ := shardEndpoint(t)
	tailer, err := cdc.New(cdc.Options{
		ShardID:         "shard0",
		Addr:            addr,
		User:            "root",
		Password:        "cachet",
		Database:        "cachet",
		Table:           "entities",
		ServerID:        serverID,
		Cache:           inv,
		Checkpoint:      cp,
		CheckpointEvery: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("cdc.New: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- tailer.Run(ctx) }()
	t.Cleanup(func() {
		tailer.Close()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("the tailer did not stop")
		}
	})

	// Give replication time to attach before the test writes anything, or the write can land
	// before the stream starts and the test would be racing the connection rather than the code.
	time.Sleep(1500 * time.Millisecond)
	return tailer
}

// uniqueVersion returns a version no previous run of this test can have written.
//
// Fixed versions make these tests pass exactly once. On a re-run the UPSERT writes values identical
// to what is already stored, MySQL emits no row event at all, and the tailer correctly sees
// nothing — so the test fails for a reason that has nothing to do with the tailer.
func uniqueVersion(offset uint64) uint64 {
	return uint64(time.Now().UnixMilli())<<16 + offset
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestTailerInvalidatesAnOutOfBandUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := openDB(t)
	rec := newRecorder()
	startTailer(ctx, t, rec, 2001, cdc.NewFileCheckpoint(filepath.Join(t.TempDir(), "p")))

	const id = 9_600_001
	version := uniqueVersion(1)

	// A write made DIRECTLY to MySQL, bypassing Cachet entirely. The engine never sees it, so the
	// synchronous write-path invalidation cannot fire. This is precisely the gap Flux exists to
	// close — and the reason CONSISTENCY.md §6 says such writes are bounded only by the CDC lag.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, 1, 0, ?, ?)
		 ON DUPLICATE KEY UPDATE payload = VALUES(payload), version = VALUES(version)`,
		id, []byte(fmt.Sprintf("out-of-band-%d", version)), version); err != nil {
		t.Fatalf("direct insert: %v", err)
	}

	key := fmt.Sprintf("entities:%d", id)
	waitFor(t, "the tailer to invalidate "+key, func() bool { return rec.versionOf(key) == version })
}

func TestTailerCarriesTheRowVersion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := openDB(t)
	rec := newRecorder()
	startTailer(ctx, t, rec, 2002, cdc.NewFileCheckpoint(filepath.Join(t.TempDir(), "p")))

	const id = 9_600_002
	key := fmt.Sprintf("entities:%d", id)

	// Successive updates must produce successive invalidation versions. An invalidation without the
	// right version cannot participate in the compare-and-set: it could only delete
	// unconditionally, which reopens the delete-versus-fill race the marker exists to close.
	base := uniqueVersion(0)
	for _, v := range []uint64{base + 1, base + 2, base + 3} {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, 1, 0, 'x', ?)
			 ON DUPLICATE KEY UPDATE version = VALUES(version)`, id, v); err != nil {
			t.Fatalf("write version %d: %v", v, err)
		}
		waitFor(t, fmt.Sprintf("version %d on %s", v, key), func() bool { return rec.versionOf(key) == v })
	}
}

func TestTailerResumesFromItsCheckpointAfterRestart(t *testing.T) {
	db := openDB(t)
	path := filepath.Join(t.TempDir(), "resume.pos")
	cp := cdc.NewFileCheckpoint(path)

	const id = 9_600_003
	key := fmt.Sprintf("entities:%d", id)

	// Phase 1: run a tailer, write, and let it checkpoint.
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := newRecorder()
	startTailer(firstCtx, t, first, 2003, cp)

	firstVersion := uniqueVersion(1)
	if _, err := db.ExecContext(firstCtx,
		`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, 1, 0, 'a', ?)
		 ON DUPLICATE KEY UPDATE payload = VALUES(payload), version = VALUES(version)`,
		id, firstVersion); err != nil {
		t.Fatalf("first write: %v", err)
	}
	waitFor(t, "the first invalidation", func() bool { return first.versionOf(key) == firstVersion })
	waitFor(t, "a checkpoint to be written", func() bool {
		_, found, err := cp.Load()
		return err == nil && found
	})

	saved, _, err := cp.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cancelFirst()
	time.Sleep(500 * time.Millisecond)

	// Phase 2: write while NO tailer is running. These events must not be lost.
	downtimeVersion := uniqueVersion(2)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, 1, 0, 'b', ?)
		 ON DUPLICATE KEY UPDATE payload = VALUES(payload), version = VALUES(version)`,
		id, downtimeVersion); err != nil {
		t.Fatalf("write during downtime: %v", err)
	}

	// Phase 3: a new tailer resumes from the checkpoint and must catch up.
	//
	// This is the whole point of durable checkpointing. Restarting from the CURRENT position
	// instead would leave a gap: every write during the downtime would never be invalidated, and
	// those keys would stay stale until their TTL with nothing recording that it happened.
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	second := newRecorder()
	startTailer(secondCtx, t, second, 2004, cdc.NewFileCheckpoint(path))

	waitFor(t, "the restarted tailer to catch up on the write it missed", func() bool {
		return second.versionOf(key) == downtimeVersion
	})

	resumed, _, err := cp.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !saved.Before(resumed) {
		t.Errorf("the checkpoint did not advance: %v then %v", saved, resumed)
	}
}

func TestTailerIgnoresOtherTables(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := openDB(t)
	rec := newRecorder()
	startTailer(ctx, t, rec, 2005, cdc.NewFileCheckpoint(filepath.Join(t.TempDir(), "p")))

	// seed_meta is written on every reseed. Invalidating on unrelated tables would turn any
	// background job into a source of cache churn.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO seed_meta (shard_id, profile, row_count, seed, checksum)
		 VALUES (99, 'tailer-test', 1, 1, 'x')
		 ON DUPLICATE KEY UPDATE row_count = row_count + 1`); err != nil {
		t.Fatalf("write to seed_meta: %v", err)
	}

	const id = 9_600_004
	version := uniqueVersion(4)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, 1, 0, 'x', ?)
		 ON DUPLICATE KEY UPDATE payload = VALUES(payload), version = VALUES(version)`,
		id, version); err != nil {
		t.Fatalf("write to entities: %v", err)
	}

	// Wait for the entities write to arrive, then confirm nothing from seed_meta did.
	waitFor(t, "the entities invalidation", func() bool {
		return rec.versionOf(fmt.Sprintf("entities:%d", id)) == version
	})

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for key := range rec.seen {
		if len(key) < 9 || key[:9] != "entities:" {
			t.Errorf("the tailer invalidated %q, which is not an entities key", key)
		}
	}
}

func TestNewRejectsAMissingServerID(t *testing.T) {
	// Two tailers sharing a server id fight over the replication connection and disconnect each
	// other in a loop, which presents as intermittent invalidation rather than as an error.
	if _, err := cdc.New(cdc.Options{
		Cache: newRecorder(), Checkpoint: cdc.NewFileCheckpoint(os.DevNull), Table: "entities",
	}); err == nil {
		t.Error("cdc.New accepted a zero server id")
	}
}
