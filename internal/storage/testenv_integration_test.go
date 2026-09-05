//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/goleak"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// TestMain enforces CONTRIBUTING.md rule 2 mechanically: no goroutine without an owner. A leaked
// connection-pool goroutine here would be a leaked goroutine per request in the engine.
func TestMain(m *testing.M) {
	code := m.Run()
	tearDownShardContainer()
	if code == 0 {
		if err := goleak.Find(
			// testcontainers' reaper client keeps a background connection for the process lifetime.
			goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		); err != nil {
			fmt.Fprintf(os.Stderr, "goroutine leak: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

var (
	shardOnce sync.Once
	shardDSN  string
	shardTC   testcontainers.Container
	shardErr  error
)

// openTestShard returns a Shard backed by a real Percona/MyRocks container.
//
// The container is started once for the package and shared: bootstrapping MyRocks costs about ten
// seconds, and paying that per test would make the suite slow enough that people stop running it —
// which is how a test suite stops protecting anything (build plan §9.2). Tests therefore use
// disjoint row ids rather than a fresh database.
//
// It deliberately mounts the SAME my.cnf and schema the compose environment uses, so this suite
// also proves that configuration boots, rather than testing a convenient fiction.
func openTestShard(ctx context.Context, t *testing.T) *storage.Shard {
	t.Helper()

	shardOnce.Do(func() { shardDSN, shardTC, shardErr = startShardContainer(ctx) })
	if shardErr != nil {
		t.Fatalf("start shard container: %v", shardErr)
	}
	_ = shardTC

	sh, err := storage.OpenShard(ctx, "shard-test", shardDSN, storage.NewClock(time.Now))
	if err != nil {
		t.Fatalf("OpenShard: %v", err)
	}
	t.Cleanup(func() {
		if err := sh.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return sh
}

func startShardContainer(ctx context.Context) (string, testcontainers.Container, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", nil, fmt.Errorf("getwd: %w", err)
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

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("start container: %w", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("container host: %w", err)
	}
	port, err := c.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return "", nil, fmt.Errorf("mapped port: %w", err)
	}

	dsn := fmt.Sprintf("root:cachet@tcp(%s:%s)/cachet?parseTime=true&interpolateParams=true", host, port.Port())
	return dsn, c, nil
}

func tearDownShardContainer() {
	if shardTC == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = shardTC.Terminate(ctx)
}

// forceRowVersion writes a row with an arbitrary version, bypassing the HLC. It exists to simulate
// a second engine instance — or an out-of-band write made directly to MySQL — and must never be
// used to set up ordinary state.
func forceRowVersion(ctx context.Context, t *testing.T, sh *storage.Shard, rec storage.Record, v storage.Version) {
	t.Helper()

	if err := storage.ForceRowVersionForTest(ctx, sh, rec, v); err != nil {
		t.Fatalf("forceRowVersion: %v", err)
	}
}
