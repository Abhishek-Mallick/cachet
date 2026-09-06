// Command flux is Cachet's CDC tailer.
//
// It streams each shard's binlog and invalidates the cache entry for every row it sees change. It is
// a BACKSTOP: the engine already invalidates synchronously on the write path, before acknowledging
// the write. Flux catches what that path cannot — writes made directly to MySQL, engine instances
// that died between commit and invalidation, and conditional updates too large to resolve exactly.
//
// Running it is what turns "eventually the TTL expires" into "bounded by the tailer's lag".
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/cdc"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/obs"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "flux: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to a YAML config file")
	stateDir := flag.String("state-dir", "./.flux", "directory for durable binlog checkpoints")
	printVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *printVersion {
		fmt.Println("flux", version)
		return nil
	}

	cfg, err := config.Load(*configPath, envMap())
	if err != nil {
		return err
	}
	log, err := obs.NewLogger(cfg.Observability, os.Stderr)
	if err != nil {
		return err
	}
	slog.SetDefault(log)

	if len(cfg.Cache.Addresses) == 0 {
		// Flux exists only to invalidate. Without a cache there is nothing for it to do, and
		// starting anyway would present a healthy process that silently accomplishes nothing.
		return errors.New("flux: no cache configured; there would be nothing to invalidate")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cacheClient, err := cache.New(ctx, cache.Options{
		Addresses: cfg.Cache.Addresses,
		TTL:       cfg.Consistency.EntryTTL,
	})
	if err != nil {
		return err
	}
	defer func() { _ = cacheClient.Close() }()

	if err := os.MkdirAll(*stateDir, 0o750); err != nil {
		return fmt.Errorf("flux: create state dir %s: %w", *stateDir, err)
	}

	var applied, rejected atomic.Int64

	g, gctx := errgroup.WithContext(ctx)
	for i, sc := range cfg.Shards {
		addr, user, password, database, err := parseDSN(sc.DSN)
		if err != nil {
			return fmt.Errorf("flux: shard %s: %w", sc.ID, err)
		}

		tailer, err := cdc.New(cdc.Options{
			ShardID:  sc.ID,
			Addr:     addr,
			User:     user,
			Password: password,
			Database: database,
			Table:    "entities",
			// Server ids must be unique across every replica and tailer attached to a MySQL
			// instance. The offset keeps them clear of the shards' own ids, which are 1..N.
			ServerID:        uint32(1000 + i),
			Cache:           cacheClient,
			Checkpoint:      cdc.NewFileCheckpoint(filepath.Join(*stateDir, sc.ID+".pos")),
			CheckpointEvery: 2 * time.Second,
			Logger:          log.With("shard", sc.ID),
			OnInvalidate: func(_ string, _ uint64, wasApplied bool) {
				if wasApplied {
					applied.Add(1)
					return
				}
				// A rejected invalidation is the NORMAL case in steady state: the engine already
				// tombstoned that key synchronously, so Flux arrives second and loses the
				// compare-and-set. A rising APPLIED rate is the interesting signal — it means the
				// write path is missing invalidations and the backstop is doing real work.
				rejected.Add(1)
			},
		})
		if err != nil {
			return err
		}
		defer tailer.Close()

		g.Go(func() error { return tailer.Run(gctx) })
		log.Info("tailing shard", "shard", sc.ID, "addr", addr)
	}

	g.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Info("invalidation summary",
					"applied", applied.Load(), "rejected_as_redundant", rejected.Load())
			case <-gctx.Done():
				return nil
			}
		}
	})

	err = g.Wait()
	log.Info("stopped", "applied", applied.Load(), "rejected_as_redundant", rejected.Load())
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// parseDSN pulls the replication parameters out of a go-sql-driver DSN.
//
// The tailer needs host, user and password separately because replication is a different protocol
// from the query connection, not a different query on it.
func parseDSN(dsn string) (addr, user, password, database string, err error) {
	creds, rest, ok := strings.Cut(dsn, "@")
	if !ok {
		return "", "", "", "", fmt.Errorf("malformed dsn %q", dsn)
	}
	user, password, _ = strings.Cut(creds, ":")

	_, rest, ok = strings.Cut(rest, "(")
	if !ok {
		return "", "", "", "", fmt.Errorf("dsn %q has no host", dsn)
	}
	addr, rest, ok = strings.Cut(rest, ")")
	if !ok {
		return "", "", "", "", fmt.Errorf("dsn %q has no host", dsn)
	}

	database = strings.TrimPrefix(rest, "/")
	if i := strings.IndexByte(database, '?'); i >= 0 {
		database = database[:i]
	}
	if database == "" {
		return "", "", "", "", fmt.Errorf("dsn %q has no database", dsn)
	}
	return addr, user, password, database, nil
}

func envMap() map[string]string {
	out := make(map[string]string)
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	return out
}
