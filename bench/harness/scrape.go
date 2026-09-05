package harness

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Counters is a snapshot of the engine's cumulative counters.
type Counters struct {
	CacheHits   float64
	CacheMisses float64
	CacheStale  float64
	OriginReads float64
}

// Scrape reads the engine's Prometheus endpoint.
//
// The cache and origin numbers come from the engine itself rather than from the load generator,
// because the load generator cannot see them: it observes latency, while the claim being tested is
// about database load (benchmarking doc §3.6).
func Scrape(ctx context.Context, url string) (Counters, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Counters{}, fmt.Errorf("harness: build scrape request: %w", err)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// Failing loudly matters: silently returning zeros would publish a 0% hit rate as though it
		// had been measured, which is worse than publishing nothing.
		return Counters{}, fmt.Errorf("harness: scrape %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return Counters{}, fmt.Errorf("harness: scrape %s: status %d", url, resp.StatusCode)
	}

	return parseCounters(resp.Body)
}

// parseCounters extracts the handful of counters this harness needs from the exposition format.
//
// This is hand-rolled rather than using prometheus/common's parser deliberately. That parser reads a
// package-level validation-scheme global which, if unset, PANICS mid-parse — a library that
// panics on well-formed input because of an unset global is not something to put on the path
// between a benchmark and its published number. The exposition format for counters is two shapes,
// both handled below, and this way the benchmark tooling carries no dependency that can fail in
// that manner.
func parseCounters(r io.Reader) (Counters, error) {
	var out Counters

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// A sample is "<name>[{labels}] <value>", so the value is everything after the last space.
		sep := strings.LastIndexByte(line, ' ')
		if sep < 0 {
			continue
		}
		series, rawValue := line[:sep], line[sep+1:]

		value, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			continue // a timestamped or malformed sample is skipped rather than failing the scrape
		}

		name, labels, _ := strings.Cut(series, "{")
		switch name {
		case "cachet_origin_reads_total":
			out.OriginReads += value
		case "cachet_cache_operations_total":
			if !strings.Contains(labels, `op="get"`) {
				continue
			}
			switch {
			case strings.Contains(labels, `result="hit"`):
				out.CacheHits += value
			case strings.Contains(labels, `result="miss"`):
				out.CacheMisses += value
			case strings.Contains(labels, `result="stale"`):
				out.CacheStale += value
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return Counters{}, fmt.Errorf("harness: read metrics: %w", err)
	}
	return out, nil
}

// HitRate computes the hit rate over the interval between two snapshots.
//
// Stale results count AGAINST the hit rate. From the reader's point of view a stale entry is a
// miss: the row was in the cache, could not be served at the requested level, and the database was
// queried anyway. Excluding stale from the denominator would report a hit rate that the origin load
// flatly contradicts.
func HitRate(before, after Counters) float64 {
	hits := after.CacheHits - before.CacheHits
	total := hits + (after.CacheMisses - before.CacheMisses) + (after.CacheStale - before.CacheStale)
	if total <= 0 {
		return 0
	}
	return hits / total
}

// OriginQPS computes database reads per second over the interval between two snapshots.
func OriginQPS(before, after Counters, window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	return (after.OriginReads - before.OriginReads) / window.Seconds()
}
