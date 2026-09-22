// Command cachet-proxy speaks the MySQL wire protocol, so an application can use Cachet without
// linking the SDK or changing a line of code.
//
// It carries WRITES as well as reads, and that is what makes it more than a read-through cache:
// inside the write it can resolve and invalidate exactly the row that changed, and maintain the
// version column the application has never heard of. A proxy that forwarded writes untouched would
// leave every invalidation to lose its compare-and-set, and the stale entries would never clear.
//
// What it cannot do is hold a session token for the caller. A connection is the session here, which
// is the same scope MySQL itself gives you: read-own-writes within a connection, and nothing
// stronger across a connection pool. Applications that need the guarantee to follow a request
// across services want the SDK.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/obs"
	"github.com/Abhishek-Mallick/cachet/internal/proxy"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "cachet-proxy: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath   = flag.String("config", "", "path to the Cachet YAML config file")
		listen       = flag.String("listen", "127.0.0.1:3307", "address to accept MySQL connections on")
		upstream     = flag.String("upstream", "", "the real database, host:port (defaults to the first shard)")
		upstreamUser = flag.String("upstream-user", "root", "username for the upstream database")
		upstreamPass = flag.String("upstream-password", "", "password for the upstream database")
		upstreamDB   = flag.String("upstream-db", "cachet", "database to connect to upstream")
		user         = flag.String("user", "cachet", "username clients present to the PROXY")
		password     = flag.String("password", "", "password clients present to the proxy")
		table        = flag.String("table", "entities", "the cached table")
		opaque       = flag.String("opaque-writes", "refuse", "what to do with writes that cannot be resolved to rows: refuse|forward")
		printVersion = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	if *printVersion {
		fmt.Println("cachet-proxy", version, "protocol", engine.ProtocolVersion)
		return nil
	}

	cfg, err := config.Load(*configPath, nil)
	if err != nil {
		return err
	}
	log, err := obs.NewLogger(cfg.Observability, os.Stderr)
	if err != nil {
		return err
	}
	slog.SetDefault(log)

	policy, err := opaquePolicy(*opaque)
	if err != nil {
		return err
	}

	// One upstream, so one shard. A proxy fans a connection out to exactly one database, and an
	// engine routing keys across three of them would look for rows that upstream does not have.
	if len(cfg.Shards) != 1 {
		return fmt.Errorf("cachet-proxy: needs exactly one shard, found %d — "+
			"a MySQL connection reaches one database, so a sharded deployment needs one proxy per shard",
			len(cfg.Shards))
	}
	if *upstream == "" {
		addr, dsnUser, dsnPass, dsnDB, err := storage.ParseDSN(cfg.Shards[0].DSN)
		if err != nil {
			return fmt.Errorf("cachet-proxy: deriving the upstream address: %w", err)
		}
		*upstream = addr
		if *upstreamUser == "root" && dsnUser != "" {
			*upstreamUser = dsnUser
		}
		if *upstreamPass == "" {
			*upstreamPass = dsnPass
		}
		if *upstreamDB == "cachet" && dsnDB != "" {
			*upstreamDB = dsnDB
		}
	}

	log.LogAttrs(context.Background(), slog.LevelInfo, "starting cachet-proxy",
		append([]slog.Attr{
			slog.String("version", version),
			slog.String("listen", *listen),
			slog.String("upstream", *upstream),
			slog.String("table", *table),
			slog.String("opaque_writes", *opaque),
		}, cfg.Consistency.LogAttrs()...)...)

	registry := prometheus.NewRegistry()
	metrics, err := obs.NewMetrics(registry)
	if err != nil {
		return err
	}
	metrics.PublishGuarantees(cfg.Consistency)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shard, err := storage.OpenShard(ctx, storage.ShardID(cfg.Shards[0].ID), cfg.Shards[0].DSN,
		storage.NewClock(time.Now))
	if err != nil {
		return err
	}
	defer func() { _ = shard.Close() }()

	router, err := storage.NewRouter([]storage.ShardID{storage.ShardID(cfg.Shards[0].ID)})
	if err != nil {
		return err
	}

	if len(cfg.Cache.Addresses) == 0 {
		return errors.New("cachet-proxy: no cache configured; without one this is a slower MySQL")
	}
	cc, err := cache.New(ctx, cache.Options{
		Fingerprint: engine.Fingerprint(),
		Addresses:   cfg.Cache.Addresses,
		TTL:         cfg.Consistency.EntryTTL,
		LeaseTTL:    cfg.Cache.Lease.TTL,
	})
	if err != nil {
		return err
	}
	defer func() { _ = cc.Close() }()

	eng, err := engine.New(engine.Options{
		Router:                  router,
		Shards:                  map[storage.ShardID]*storage.Shard{storage.ShardID(cfg.Shards[0].ID): shard},
		Cache:                   cc,
		MaxSessionShards:        cfg.Consistency.MaxSessionShards,
		MaxAffectedKeys:         cfg.Consistency.MaxAffectedKeys,
		CDCLagBound:             cfg.Consistency.CDCLagBound,
		MaxClockSkew:            cfg.Consistency.MaxClockSkew,
		SynchronousInvalidation: cfg.Consistency.SynchronousInvalidation,
		Version:                 version,
		Metrics:                 metrics,
		Logger:                  log,
	})
	if err != nil {
		return err
	}

	srv, err := proxy.New(proxy.Options{
		Listen:           *listen,
		UpstreamAddr:     *upstream,
		UpstreamUser:     *upstreamUser,
		UpstreamPassword: *upstreamPass,
		UpstreamDB:       *upstreamDB,
		User:             *user,
		Password:         *password,
		CachedTable:      *table,
		Engine:           eng,
		Cache:            cc,
		OpaqueWrites:     policy,
		Logger:           log,
	})
	if err != nil {
		return err
	}
	log.Info("accepting MySQL connections", "address", srv.Addr().String())

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return srv.Serve(gctx) })
	g.Go(func() error { return obs.ServeMetrics(gctx, cfg.Observability.MetricsListen, registry, log) })

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("stopped")
	return nil
}

func opaquePolicy(s string) (proxy.OpaqueWritePolicy, error) {
	switch s {
	case "refuse":
		return proxy.RefuseOpaqueWrites, nil
	case "forward":
		return proxy.ForwardOpaqueWrites, nil
	default:
		return 0, fmt.Errorf("cachet-proxy: -opaque-writes must be refuse or forward, got %q", s)
	}
}
