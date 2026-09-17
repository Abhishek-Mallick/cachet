// Command sextant is Cachet's consistency verifier.
//
// It watches a cache and the database behind it, decides when the difference between them is a real
// violation rather than an in-flight race, and publishes the answer as a measured consistency
// figure per level. That number is the product's central claim: every other cache in this category
// asks to be trusted, and this is the part that lets Cachet be checked instead.
//
// SHADOW MODE (-shadow) is the reason to run it first. The same detection and tracing loops, pointed
// at a deployment your application is not reading through: Sextant reports what your consistency
// WOULD have been, with no application change and no risk. Nobody adopts a cache on promises, and
// letting a team measure their own consistency before touching a line of code is an offer no
// competitor in this category can make — because none of them can measure consistency at all.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/cdc"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/sextant"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "sextant: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to a YAML config file")
	shadow := flag.Bool("shadow", false,
		"observe only: report what consistency WOULD have been, for a deployment no application reads through")
	interval := flag.Duration("interval", time.Second, "time between sampling rounds")
	batch := flag.Int("batch", 100, "keys checked per round")
	window := flag.Duration("window", 5*time.Minute, "rolling window the consistency figure is measured over")
	candidates := flag.Int("candidates", 10_000, "how many recently-written keys stay eligible for checking")
	printVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *printVersion {
		fmt.Println("sextant", version)
		return nil
	}

	cfg, err := config.Load(*configPath, envMap())
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if len(cfg.Cache.Addresses) == 0 {
		// Without a cache there is nothing whose consistency could be in question, and a verifier
		// reporting a perfect score for a system it is not watching is the worst possible output.
		return errors.New("sextant: no cache configured; there would be nothing to verify")
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cacheClient, err := cache.New(ctx, cache.Options{
		Addresses: cfg.Cache.Addresses,
		TTL:       cfg.Consistency.EntryTTL,
		LeaseTTL:  cfg.Cache.Lease.TTL,
	})
	if err != nil {
		return err
	}
	defer func() { _ = cacheClient.Close() }()

	shards := make(map[storage.ShardID]*storage.Shard, len(cfg.Shards))
	ids := make([]storage.ShardID, 0, len(cfg.Shards))
	for _, sc := range cfg.Shards {
		id := storage.ShardID(sc.ID)
		sh, err := storage.OpenShard(ctx, id, sc.DSN, storage.NewClock(time.Now))
		if err != nil {
			return err
		}
		defer func() { _ = sh.Close() }()
		shards[id] = sh
		ids = append(ids, id)
	}
	router, err := storage.NewRouter(ids)
	if err != nil {
		return err
	}

	// The propagation bound is summed from the engine's own guarantee settings, not tuned here. A
	// verifier with its own idea of the threshold would drift from the guarantee the moment anyone
	// changed a setting, and nothing would report the drift.
	bound := sextant.NewPropagationBound(
		cfg.Consistency.WritePathInvalidationBudget,
		cfg.Consistency.CDCLagBound,
		cfg.Consistency.MaxClockSkew,
	)

	slo := sextant.NewSLO(*window)
	tracer := sextant.NewTracer(sextant.TracerOptions{})
	keys := sextant.NewRecentKeys(*candidates)

	verifier, err := sextant.NewVerifier(sextant.VerifierOptions{
		Cache:    sextant.NewCacheAdapter(cacheClient),
		Origin:   sextant.NewOriginAdapter(router, shards),
		Keys:     keys,
		Bound:    bound,
		SLO:      slo,
		Tracer:   tracer,
		Interval: *interval,
		Batch:    *batch,
		Shadow:   *shadow,
		Logger:   log,
		OnViolation: func(v sextant.Violation) {
			// The violation and its trace together. A monitor logs that something was stale; this
			// logs what happened to the key and from which path, which is the only form in which
			// the finding is actionable.
			log.Warn("consistency violation", "violation", v.String(), "shadow", *shadow)
			for _, e := range v.Trace {
				log.Warn("  trace", "event", e.String())
			}
		},
	})
	if err != nil {
		return err
	}

	// Sextant learns which keys are worth checking by tailing the binlog. That is the same stream
	// Flux invalidates from, which is deliberate: the verifier watches the write stream rather than
	// being told about writes by the engine, so it observes writes the engine never saw —
	// migrations, admin scripts — and cannot be lied to by the component it is checking.
	go tailForCandidates(ctx, cfg, keys, tracer, log)

	registry := prometheus.NewRegistry()
	registerSLO(registry, slo, verifier)
	go serveMetrics(ctx, cfg.Observability.MetricsListen, registry, log)

	if *shadow {
		log.Info("running in SHADOW mode: reporting what consistency would have been; " +
			"no application traffic is served from this deployment")
	}
	return verifier.Run(ctx)
}

// tailForCandidates feeds recently-written keys to the verifier, and their mutations to the tracer.
func tailForCandidates(ctx context.Context, cfg config.Config, keys *sextant.RecentKeys, tracer *sextant.Tracer, log *slog.Logger) {
	for i, sc := range cfg.Shards {
		shardCfg := sc
		// Server ids offset well clear of Flux's, since two replication clients sharing an id fight
		// over the connection.
		serverID := uint32(6000 + i)
		go func() {
			addr, user, password, database, err := storage.ParseDSN(shardCfg.DSN)
			if err != nil {
				log.Warn("sextant: candidate tailer not started", "shard", shardCfg.ID, "err", err)
				return
			}
			tailer, err := cdc.New(cdc.Options{
				ShardID:         shardCfg.ID,
				Addr:            addr,
				User:            user,
				Password:        password,
				Database:        database,
				Table:           "entities",
				ServerID:        serverID,
				Cache:           observeOnly{keys: keys, tracer: tracer, shard: shardCfg.ID},
				Checkpoint:      noCheckpoint{},
				CheckpointEvery: time.Minute,
				Logger:          log,
			})
			if err != nil {
				log.Warn("sextant: candidate tailer not started", "shard", shardCfg.ID, "err", err)
				return
			}
			if err := tailer.Run(ctx); err != nil && ctx.Err() == nil {
				log.Warn("sextant: candidate tailer stopped", "shard", shardCfg.ID, "err", err)
			}
		}()
	}
}

// observeOnly satisfies the tailer's Invalidator interface without invalidating anything.
//
// This is what makes Sextant an observer rather than a participant. The tailer's contract is
// "tombstone this key"; Sextant records that the key changed and returns without touching the
// cache. A verifier that invalidated entries would be repairing the very staleness it is supposed
// to be measuring, and its own number would be the evidence it had done so.
type observeOnly struct {
	keys   *sextant.RecentKeys
	tracer *sextant.Tracer
	shard  string
}

func (o observeOnly) Tombstone(_ context.Context, key string, version uint64) (bool, error) {
	o.keys.Touch(key)
	o.tracer.Record(key, sextant.Event{
		Op:      sextant.OpTombstone,
		Version: version,
		At:      time.Now(),
		Actor:   o.shard,
		Source:  sextant.SourceCDC,
	})
	// Reporting "not applied" is the honest answer: nothing was applied.
	return false, nil
}

// noCheckpoint discards tailer positions.
//
// Sextant samples the present rather than replaying the past: resuming from an old position after a
// restart would make it check keys whose staleness, if any, has long since been reported or
// resolved. Flux is the component that must not lose its place; this one must not be anchored to it.
type noCheckpoint struct{}

func (noCheckpoint) Load() (cdc.Position, bool, error) { return cdc.Position{}, false, nil }
func (noCheckpoint) Save(cdc.Position) error           { return nil }

// registerSLO exports the measured consistency figure per level.
func registerSLO(reg *prometheus.Registry, slo *sextant.SLO, v *sextant.Verifier) {
	levels := []consistency.Level{consistency.Session, consistency.Bounded, consistency.Eventual}

	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "cachet", Subsystem: "sextant", Name: "shadow_mode",
		Help: "1 when observing a deployment that serves no application traffic.",
	}, func() float64 {
		if v.Shadow() {
			return 1
		}
		return 0
	}))

	for _, level := range levels {
		l := level
		labels := prometheus.Labels{"level": l.String()}

		// Nines are gauged rather than the raw fraction, because that is how this gets discussed —
		// and because a fraction rounded for display hides the difference between 0.999 and 0.99999
		// exactly where it matters most.
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "cachet", Subsystem: "sextant", Name: "consistency_nines",
			Help: "Measured consistency per level, in nines. Absent until observations exist.", ConstLabels: labels,
		}, func() float64 { return slo.Report(l, time.Now()).Nines }))

		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "cachet", Subsystem: "sextant", Name: "observations",
			Help: "Observations in the current window. Zero means the figure above is not evidence.", ConstLabels: labels,
		}, func() float64 { return float64(slo.Report(l, time.Now()).Observations) }))

		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "cachet", Subsystem: "sextant", Name: "violations",
			Help: "Violations in the current window.", ConstLabels: labels,
		}, func() float64 { return float64(slo.Report(l, time.Now()).Violations) }))
	}
}

func serveMetrics(ctx context.Context, addr string, reg *prometheus.Registry, log *slog.Logger) {
	if addr == "" {
		addr = ":9101"
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		// WithoutCancel, not Background: the shutdown must outlive the cancellation that triggered
		// it — a context already cancelled would abort the drain instantly — while still inheriting
		// any values the parent carries.
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	log.Info("sextant metrics listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("sextant metrics server stopped", "err", err)
	}
}

func envMap() map[string]string {
	out := make(map[string]string)
	for _, kv := range os.Environ() {
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				out[kv[:i]] = kv[i+1:]
				break
			}
		}
	}
	return out
}
