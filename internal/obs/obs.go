// Package obs wires Cachet's metrics, logging and tracing.
//
// Instrumentation is present from the first request rather than added once something goes wrong:
// a request path with no metrics is a path nobody can debug in production, and retrofitting them
// means re-running every benchmark taken before they existed.
package obs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/Abhishek-Mallick/cachet/internal/config"
)

// namespace prefixes every metric Cachet exports.
const namespace = "cachet"

// Metrics holds Cachet's Prometheus collectors.
type Metrics struct {
	requests  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	inFlight  *prometheus.GaugeVec
	guarantee *prometheus.GaugeVec
	cacheOps  *prometheus.CounterVec
	origin    prometheus.Counter
}

// NewMetrics registers Cachet's collectors on reg.
//
// The registry is a parameter rather than the global default so that tests can assert on a clean
// one and cannot interfere with each other — and so that a duplicate registration is a returned
// error naming the collector rather than a panic during startup wiring.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "requests_total",
			Help:      "Total gRPC requests by method and response code.",
		}, []string{"method", "code"}),

		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "request_duration_seconds",
			Help:      "Request latency by method.",
			// Buckets start at 100µs because a cache hit's whole budget is a few hundred
			// microseconds (ADR 0004). Prometheus' defaults start at 5ms, which would put every
			// cache hit in the first bucket and make the number this project is judged on invisible.
			Buckets: []float64{
				0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
				0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
			},
		}, []string{"method"}),

		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "requests_in_flight",
			Help:      "Requests currently being served, by method.",
		}, []string{"method"}),

		cacheOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "cache_operations_total",
			Help:      "Cache operations by op and result. Results: hit, miss, stale, error, ok.",
		}, []string{"op", "result"}),

		origin: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "origin_reads_total",
			Help:      "Reads that reached the database. This is the real cost metric for the caching claim.",
		}),

		guarantee: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "guarantee_setting",
			Help:      "Configured values of the settings that change Cachet's stated guarantees. Durations are in seconds.",
		}, []string{"setting"}),
	}

	for _, c := range []prometheus.Collector{m.requests, m.duration, m.inFlight, m.guarantee, m.cacheOps, m.origin} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("obs: register collector: %w", err)
		}
	}
	return m, nil
}

// RequestsTotal exposes the request counter for assertions.
func (m *Metrics) RequestsTotal() *prometheus.CounterVec { return m.requests }

// GuaranteeSetting exposes the guarantee gauge for assertions.
func (m *Metrics) GuaranteeSetting() *prometheus.GaugeVec { return m.guarantee }

// CacheOps exposes the cache counter for assertions.
func (m *Metrics) CacheOps() *prometheus.CounterVec { return m.cacheOps }

// OriginReads exposes the origin counter for assertions.
func (m *Metrics) OriginReads() prometheus.Counter { return m.origin }

// RecordCacheOp counts one cache operation.
//
// "stale" is a distinct result from "miss" on purpose: a miss means the cache did not hold the row,
// while a stale result means it did but the entry was not fresh enough for the level requested. The
// ratio between them says whether a level's cost comes from cache capacity or from the guarantee
// itself — more memory fixes one and not the other.
//
// The nil receiver is a no-op so that components can take metrics as an option and tests need not
// wire a registry to exercise them.
func (m *Metrics) RecordCacheOp(op, result string) {
	if m == nil {
		return
	}
	m.cacheOps.WithLabelValues(op, result).Inc()
}

// RecordOriginRead counts one read that reached the database.
//
// This is the metric the caching claim actually rests on: client latency may barely move while
// origin load collapses, and origin load is what costs money (benchmarking doc §3.6).
func (m *Metrics) RecordOriginRead() {
	if m == nil {
		return
	}
	m.origin.Inc()
}

// UnaryInterceptor records RED metrics for every request.
func (m *Metrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		method := shortMethod(info.FullMethod)

		m.inFlight.WithLabelValues(method).Inc()
		start := time.Now()

		resp, err := handler(ctx, req)

		m.inFlight.WithLabelValues(method).Dec()
		m.duration.WithLabelValues(method).Observe(time.Since(start).Seconds())
		// Labelling by the actual gRPC code, not a boolean, is what separates "a client sent
		// nonsense" from "a shard is down" — opposite operational problems that a single error
		// counter would merge into one useless line.
		m.requests.WithLabelValues(method, status.Code(err).String()).Inc()

		return resp, err
	}
}

// PublishGuarantees exports the settings that change what Cachet promises.
//
// CONSISTENCY.md §9 requires these to be visible in a running system: a guarantee you cannot read
// off a dashboard is one nobody can verify was in force during an incident.
func (m *Metrics) PublishGuarantees(c config.Consistency) {
	for name, v := range c.GuaranteeSettings() {
		switch typed := v.(type) {
		case time.Duration:
			m.guarantee.WithLabelValues(name).Set(typed.Seconds())
		case int:
			m.guarantee.WithLabelValues(name).Set(float64(typed))
		case bool:
			var v float64
			if typed {
				v = 1
			}
			m.guarantee.WithLabelValues(name).Set(v)
		default:
			// Every guarantee setting must be exportable. Silently skipping an unknown type would
			// let a new setting be added and never appear, which is the failure this gauge exists
			// to prevent — so it is recorded as NaN, which is visibly wrong on a dashboard.
			m.guarantee.WithLabelValues(name).Set(nan())
		}
	}
}

func nan() float64 {
	var zero float64
	return zero / zero
}

// shortMethod turns "/cachet.v1.CacheService/Get" into "Get", keeping label cardinality low and
// dashboards readable.
func shortMethod(full string) string {
	if i := strings.LastIndex(full, "/"); i >= 0 && i+1 < len(full) {
		return full[i+1:]
	}
	return full
}

// NewLogger builds the process logger.
//
// An unknown level is an error rather than a silent fallback: a typo in production would otherwise
// change what gets logged, and it would be discovered during the incident where the missing lines
// were needed.
func NewLogger(cfg config.Observability, w io.Writer) (*slog.Logger, error) {
	var level slog.Level
	switch strings.ToLower(strings.TrimSpace(cfg.LogLevel)) {
	case "debug":
		level = slog.LevelDebug
	case "info", "":
		level = slog.LevelInfo
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("obs: unknown log level %q (want debug, info, warn or error)", cfg.LogLevel)
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(cfg.LogFormat)) {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	case "text", "":
		handler = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("obs: unknown log format %q (want text or json)", cfg.LogFormat)
	}

	log := slog.New(handler)
	if name := cfg.ServiceName; name != "" {
		log = log.With("service", name)
	}
	return log, nil
}

// ServeMetrics runs the Prometheus scrape endpoint until ctx is cancelled.
//
// It is a separate listener from the gRPC data plane on purpose: metrics must remain scrapable when
// the data plane is saturated or draining, which is exactly when someone is looking at them.
func ServeMetrics(ctx context.Context, addr string, gatherer prometheus.Gatherer, log *slog.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("metrics listening", "address", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("obs: serve metrics: %w", err)
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("obs: shut down metrics: %w", err)
		}
		return <-errc
	}
}
