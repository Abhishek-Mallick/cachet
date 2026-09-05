// Command cachet runs the Cachet query engine.
//
// One binary, three topologies (ADR 0004). Which one you get is decided entirely by the listen
// addresses: a Unix socket for the sidecar, a TCP port for the service tier. There is no build flag
// and no separate binary, because a topology that is a product variant is a topology that drifts.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/obs"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// version is stamped at build time via -ldflags. It is reported in the handshake so a support
// conversation starts from what is actually deployed rather than from what someone believes is.
var version = "dev"

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet if configuration failed, so this last-resort path writes
		// directly to stderr rather than risking a nil dereference while reporting an error.
		fmt.Fprintf(os.Stderr, "cachet: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to a YAML config file")
	printVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *printVersion {
		fmt.Println("cachet", version, "protocol", engine.ProtocolVersion)
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

	// The settings that change what Cachet promises are logged at boot, every boot
	// (CONSISTENCY.md §9). An operator reading an incident timeline must be able to see which
	// guarantees were actually in force, not look them up in a config repository's history.
	log.LogAttrs(context.Background(), slog.LevelInfo, "starting cachet",
		append([]slog.Attr{
			slog.String("version", version),
			slog.String("protocol", engine.ProtocolVersion),
			slog.String("default_level", cfg.Level().String()),
		}, cfg.Consistency.LogAttrs()...)...)

	registry := prometheus.NewRegistry()
	metrics, err := obs.NewMetrics(registry)
	if err != nil {
		return err
	}
	metrics.PublishGuarantees(cfg.Consistency)

	// SIGTERM is what a container orchestrator sends. Handling it is what makes a rolling deploy
	// lossless rather than a burst of failed requests.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shards, router, err := openShards(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer closeShards(shards, log)

	eng, err := engine.New(engine.Options{
		Router:           router,
		Shards:           shards,
		MaxSessionShards: cfg.Consistency.MaxSessionShards,
		Version:          version,
		Logger:           log,
	})
	if err != nil {
		return err
	}

	srv, err := engine.NewServer(ctx, eng, engine.ServerOptions{
		Listen:            cfg.Listen,
		DrainTimeout:      cfg.Shutdown.DrainTimeout,
		Logger:            log,
		UnaryInterceptors: []grpc.UnaryServerInterceptor{metrics.UnaryInterceptor()},
	})
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return srv.Serve(gctx) })
	g.Go(func() error { return obs.ServeMetrics(gctx, cfg.Observability.MetricsListen, registry, log) })

	err = g.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("stopped")
	return nil
}

// openShards connects to every configured shard, closing the ones already open if any fails.
//
// A partial connection set is never returned: an engine serving three of four shards would answer a
// quarter of its traffic with errors while reporting itself healthy, which is worse than not
// starting.
func openShards(ctx context.Context, cfg config.Config, log *slog.Logger) (
	map[storage.ShardID]*storage.Shard, *storage.Router, error,
) {
	shards := make(map[storage.ShardID]*storage.Shard, len(cfg.Shards))
	ids := make([]storage.ShardID, 0, len(cfg.Shards))

	for _, sc := range cfg.Shards {
		id := storage.ShardID(sc.ID)

		// Each shard gets its own clock, because each shard's versions are its own sequence and
		// comparing versions across shards is a bug (ADR 0003).
		sh, err := storage.OpenShard(ctx, id, sc.DSN, storage.NewClock(time.Now))
		if err != nil {
			closeShards(shards, log)
			return nil, nil, err
		}
		shards[id] = sh
		ids = append(ids, id)
		log.Info("shard connected", "shard", id)
	}

	router, err := storage.NewRouter(ids)
	if err != nil {
		closeShards(shards, log)
		return nil, nil, err
	}
	return shards, router, nil
}

func closeShards(shards map[storage.ShardID]*storage.Shard, log *slog.Logger) {
	for id, sh := range shards {
		if err := sh.Close(); err != nil {
			log.Warn("closing shard failed", "shard", id, "err", err)
		}
	}
}

// envMap snapshots the process environment for config overrides.
func envMap() map[string]string {
	out := make(map[string]string)
	for _, kv := range os.Environ() {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	return out
}
