package harness_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Abhishek-Mallick/cachet/bench/harness"
)

const sampleMetrics = `# HELP cachet_cache_operations_total Cache operations.
# TYPE cachet_cache_operations_total counter
cachet_cache_operations_total{op="get",result="hit"} 900
cachet_cache_operations_total{op="get",result="miss"} 80
cachet_cache_operations_total{op="get",result="stale"} 20
cachet_cache_operations_total{op="set",result="ok"} 100
# HELP cachet_origin_reads_total Reads that reached the database.
# TYPE cachet_origin_reads_total counter
cachet_origin_reads_total 100
`

func serveMetrics(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestScrapeReadsCacheAndOriginCounters(t *testing.T) {
	t.Parallel()

	got, err := harness.Scrape(context.Background(), serveMetrics(t, sampleMetrics))
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}

	if got.CacheHits != 900 || got.CacheMisses != 80 || got.CacheStale != 20 {
		t.Errorf("cache counters = %v/%v/%v, want 900/80/20", got.CacheHits, got.CacheMisses, got.CacheStale)
	}
	if got.OriginReads != 100 {
		t.Errorf("origin reads = %v, want 100", got.OriginReads)
	}
}

func TestHitRateCountsStaleMissesAgainstIt(t *testing.T) {
	t.Parallel()

	before := harness.Counters{}
	after := harness.Counters{CacheHits: 900, CacheMisses: 80, CacheStale: 20, OriginReads: 100}

	// A stale entry is a miss from the reader's point of view: the row was in the cache but could
	// not be served, and the database was queried anyway. Excluding stale from the denominator
	// would report a hit rate the origin load flatly contradicts.
	rate := harness.HitRate(before, after)
	if want := 0.90; rate != want {
		t.Errorf("HitRate = %v, want %v", rate, want)
	}
}

func TestHitRateOfNoTrafficIsZeroNotNaN(t *testing.T) {
	t.Parallel()

	if got := harness.HitRate(harness.Counters{}, harness.Counters{}); got != 0 {
		t.Errorf("HitRate with no traffic = %v, want 0", got)
	}
}

func TestScrapeReportsAnUnreachableEndpoint(t *testing.T) {
	t.Parallel()

	// Silently returning zeros would publish a 0% hit rate as though it were measured, which is
	// worse than publishing nothing.
	if _, err := harness.Scrape(context.Background(), "http://127.0.0.1:1/metrics"); err == nil {
		t.Error("Scrape succeeded against an unreachable endpoint")
	}
}
