package obs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/obs"
)

func newMetrics(t *testing.T) (*obs.Metrics, *prometheus.Registry) {
	t.Helper()

	reg := prometheus.NewRegistry()
	m, err := obs.NewMetrics(reg)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	return m, reg
}

func TestEveryRequestIsCounted(t *testing.T) {
	t.Parallel()

	m, reg := newMetrics(t)
	interceptor := m.UnaryInterceptor()

	info := &grpc.UnaryServerInfo{FullMethod: "/cachet.v1.CacheService/Get"}
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	for i := 0; i < 3; i++ {
		if _, err := interceptor(context.Background(), nil, info, handler); err != nil {
			t.Fatalf("interceptor: %v", err)
		}
	}

	// RED metrics on every request path are a merge requirement, not a nice-to-have
	// (CONTRIBUTING.md rule 14): a path with no metrics is a path nobody can debug in production.
	got := testutil.ToFloat64(m.RequestsTotal().WithLabelValues("Get", "OK"))
	if got != 3 {
		t.Errorf("requests_total{method=Get,code=OK} = %v, want 3", got)
	}
	if n := testutil.CollectAndCount(reg, "cachet_request_duration_seconds"); n == 0 {
		t.Error("no request duration observations were recorded")
	}
}

func TestErrorsAreCountedUnderTheirCode(t *testing.T) {
	t.Parallel()

	m, _ := newMetrics(t)
	interceptor := m.UnaryInterceptor()

	info := &grpc.UnaryServerInfo{FullMethod: "/cachet.v1.CacheService/Put"}
	handler := func(context.Context, any) (any, error) {
		return nil, status.Error(codes.InvalidArgument, "bad key")
	}

	if _, err := interceptor(context.Background(), nil, info, handler); err == nil {
		t.Fatal("interceptor swallowed the handler's error")
	}

	// Bucketing every failure as "error" would hide the difference between a client sending
	// nonsense and a shard being down — which are opposite operational problems.
	if got := testutil.ToFloat64(m.RequestsTotal().WithLabelValues("Put", "InvalidArgument")); got != 1 {
		t.Errorf("requests_total{method=Put,code=InvalidArgument} = %v, want 1", got)
	}
}

func TestNonStatusErrorsAreCountedAsUnknown(t *testing.T) {
	t.Parallel()

	m, _ := newMetrics(t)
	interceptor := m.UnaryInterceptor()

	info := &grpc.UnaryServerInfo{FullMethod: "/cachet.v1.CacheService/Get"}
	handler := func(context.Context, any) (any, error) { return nil, errors.New("boom") }

	if _, err := interceptor(context.Background(), nil, info, handler); err == nil {
		t.Fatal("interceptor swallowed the handler's error")
	}
	if got := testutil.ToFloat64(m.RequestsTotal().WithLabelValues("Get", "Unknown")); got != 1 {
		t.Errorf("requests_total{method=Get,code=Unknown} = %v, want 1", got)
	}
}

func TestGuaranteeSettingsAreExportedAsGauges(t *testing.T) {
	t.Parallel()

	m, reg := newMetrics(t)
	cfg := config.Default().Consistency

	m.PublishGuarantees(cfg)

	// CONSISTENCY.md §9 requires these to be visible in a running system. A guarantee you cannot
	// read off a dashboard is a guarantee nobody can verify was in force during an incident.
	out, err := testutil.GatherAndLint(reg)
	if err != nil {
		t.Fatalf("GatherAndLint: %v", err)
	}
	_ = out

	if got := testutil.ToFloat64(m.GuaranteeSetting().WithLabelValues("max_clock_skew")); got != cfg.MaxClockSkew.Seconds() {
		t.Errorf("max_clock_skew gauge = %v, want %v", got, cfg.MaxClockSkew.Seconds())
	}
	if got := testutil.ToFloat64(m.GuaranteeSetting().WithLabelValues("max_affected_keys")); got != float64(cfg.MaxAffectedKeys) {
		t.Errorf("max_affected_keys gauge = %v, want %v", got, float64(cfg.MaxAffectedKeys))
	}
	if n := testutil.CollectAndCount(reg, "cachet_guarantee_setting"); n != len(cfg.GuaranteeSettings()) {
		t.Errorf("exported %d guarantee gauges, want %d — every one must be visible", n, len(cfg.GuaranteeSettings()))
	}
}

func TestMetricsCannotBeRegisteredTwice(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	if _, err := obs.NewMetrics(reg); err != nil {
		t.Fatalf("first NewMetrics: %v", err)
	}
	// Duplicate registration must be an error rather than a panic: it is a wiring mistake, and a
	// panic during startup wiring is harder to diagnose than a returned error naming the collector.
	if _, err := obs.NewMetrics(reg); err == nil {
		t.Error("NewMetrics succeeded twice on the same registry")
	}
}

func TestLoggerHonoursTheConfiguredLevel(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	log, err := obs.NewLogger(config.Observability{LogLevel: "warn", LogFormat: "json"}, &sb)
	if err != nil {
		t.Fatalf("NewLogger: %v", err)
	}

	log.Info("this should be filtered")
	log.Warn("this should appear")

	out := sb.String()
	if strings.Contains(out, "filtered") {
		t.Error("info output appeared at warn level")
	}
	if !strings.Contains(out, "appear") {
		t.Error("warn output was filtered")
	}
	if !strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Errorf("json format produced non-JSON output: %q", out)
	}
}

func TestUnknownLogLevelIsRejected(t *testing.T) {
	t.Parallel()

	// Falling back to a default would mean a typo in production silently changes what gets logged,
	// discovered during the incident where the missing lines were needed.
	if _, err := obs.NewLogger(config.Observability{LogLevel: "chatty"}, &strings.Builder{}); err == nil {
		t.Error("NewLogger accepted an unknown log level")
	}
}

func TestDurationIsObservedForSlowHandlers(t *testing.T) {
	t.Parallel()

	m, _ := newMetrics(t)
	interceptor := m.UnaryInterceptor()

	info := &grpc.UnaryServerInfo{FullMethod: "/cachet.v1.CacheService/Get"}
	handler := func(context.Context, any) (any, error) {
		time.Sleep(5 * time.Millisecond)
		return "ok", nil
	}

	if _, err := interceptor(context.Background(), nil, info, handler); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if got := testutil.ToFloat64(m.RequestsTotal().WithLabelValues("Get", "OK")); got != 1 {
		t.Errorf("requests_total = %v, want 1", got)
	}
}

func TestCacheOutcomesAreCountedSeparately(t *testing.T) {
	t.Parallel()

	m, _ := newMetrics(t)

	m.RecordCacheOp("get", "hit")
	m.RecordCacheOp("get", "hit")
	m.RecordCacheOp("get", "miss")
	m.RecordCacheOp("get", "stale")

	// "stale" is counted apart from "miss" deliberately. A plain miss means the cache did not have
	// the row; a stale miss means it did, but the entry was not fresh enough for the level asked
	// for. The ratio between them says whether a level's cost comes from cache capacity or from the
	// guarantee itself — one is fixed by more memory, the other is not.
	for _, tc := range []struct {
		result string
		want   float64
	}{{"hit", 2}, {"miss", 1}, {"stale", 1}} {
		if got := testutil.ToFloat64(m.CacheOps().WithLabelValues("get", tc.result)); got != tc.want {
			t.Errorf("cache_operations_total{op=get,result=%s} = %v, want %v", tc.result, got, tc.want)
		}
	}
}

func TestOriginReadsAreCounted(t *testing.T) {
	t.Parallel()

	m, _ := newMetrics(t)
	for i := 0; i < 5; i++ {
		m.RecordOriginRead()
	}

	// Origin QPS is the real cost metric for the caching claim: client latency may barely move
	// while database load collapses (benchmarking doc §3.6).
	if got := testutil.ToFloat64(m.OriginReads()); got != 5 {
		t.Errorf("origin_reads_total = %v, want 5", got)
	}
}

func TestNilMetricsAreSafeToCall(t *testing.T) {
	t.Parallel()

	// The engine takes metrics as an option so tests need not wire a registry. A nil receiver that
	// panics would make every such test a landmine.
	var m *obs.Metrics
	m.RecordCacheOp("get", "hit")
	m.RecordOriginRead()
}
