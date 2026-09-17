package ctl

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/bench/harness"
)

// BenchOptions configures a quick smoke benchmark.
type BenchOptions struct {
	Target  string
	Rate    int
	Workers int
	Warmup  time.Duration
	Measure time.Duration

	// Keys is how many distinct rows the workload touches, and ReadFraction how much of the mix is
	// reads.
	Keys         uint64
	KeyBase      uint64
	ReadFraction float64
}

// BenchReport is what a quick benchmark found.
type BenchReport struct {
	Target  string        `json:"target"`
	Rate    int           `json:"rate"`
	Measure time.Duration `json:"measure"`

	Requests   int64   `json:"requests"`
	Errors     int64   `json:"errors"`
	Throughput float64 `json:"throughput_rps"`

	P50 time.Duration `json:"p50"`
	P90 time.Duration `json:"p90"`
	P99 time.Duration `json:"p99"`

	// HitRate and OriginQPS come from the engine's own counters, not from anything this tool
	// inferred. The cost metric the caching claim rests on is origin load, and a client cannot see
	// it from the outside.
	HitRate   float64 `json:"hit_rate"`
	OriginQPS float64 `json:"origin_qps"`
	Scraped   bool    `json:"metrics_scraped"`

	// Behind is the harness' own honesty check: requests the generator dispatched late. A non-zero
	// value means the run measured the generator rather than the system.
	Behind int64 `json:"behind"`
}

// BenchQuick runs a short read-heavy workload against a live cluster and reports what it saw.
//
// This is step 2 of the adoption funnel and the reason it exists as a subcommand rather than a
// document: a stranger evaluating Cachet should be able to get THEIR numbers, from THEIR database,
// without cloning this repository or learning the benchmark harness. Nobody adopts a cache on
// somebody else's benchmark.
//
// It is deliberately a SMOKE test and says so in its own output. The full harness exists for
// publishable figures, with three runs, declared parameters and a provenance line; this is the
// thirty-second version whose job is to answer "does this do anything for me at all".
func BenchQuick(ctx context.Context, opts BenchOptions, metricsURL string) (BenchReport, error) {
	if opts.Rate <= 0 {
		opts.Rate = 200
	}
	if opts.Workers <= 0 {
		opts.Workers = 32
	}
	if opts.Measure <= 0 {
		opts.Measure = 15 * time.Second
	}
	if opts.Keys == 0 {
		opts.Keys = 1000
	}
	if opts.ReadFraction <= 0 {
		opts.ReadFraction = 0.95
	}

	conn, err := grpc.NewClient(dialTarget(opts.Target), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return BenchReport{}, fmt.Errorf("ctl: dial %s: %w", opts.Target, err)
	}
	defer func() { _ = conn.Close() }()
	client := cachetv1.NewCacheServiceClient(conn)

	before, beforeErr := harness.Scrape(ctx, metricsURL)

	// Zipfian, because a uniform workload has no hot set and therefore nothing a cache can help
	// with — it would measure the cache's overhead and none of its benefit.
	zipf, err := harness.NewZipfian(opts.Keys, 0.99, 42)
	if err != nil {
		return BenchReport{}, fmt.Errorf("ctl: bench workload: %w", err)
	}
	rng := rand.New(rand.NewPCG(7, 11))

	driver := harness.Driver{
		Rate: opts.Rate, Workers: opts.Workers,
		Warmup: opts.Warmup, Measure: opts.Measure,
		Op: func(ctx context.Context, i uint64) error {
			id := opts.KeyBase + zipf.Next()
			key := fmt.Sprintf("entities:%d", id)

			if rng.Float64() >= opts.ReadFraction {
				_, err := client.Put(ctx, &cachetv1.PutRequest{
					Key:    key,
					Record: &cachetv1.Record{TenantId: 1, Payload: []byte("bench")},
				})
				return err
			}
			_, err := client.Get(ctx, &cachetv1.GetRequest{Key: key})
			return err
		},
	}

	res, err := driver.Run(ctx)
	if err != nil {
		return BenchReport{}, fmt.Errorf("ctl: bench: %w", err)
	}

	report := BenchReport{
		Target: opts.Target, Rate: opts.Rate, Measure: opts.Measure,
		Requests: res.Requests, Errors: res.Errors, Throughput: res.Throughput,
		P50:    res.Read.Percentile(50),
		P90:    res.Read.Percentile(90),
		P99:    res.Read.Percentile(99),
		Behind: res.Behind,
	}

	after, afterErr := harness.Scrape(ctx, metricsURL)
	if beforeErr == nil && afterErr == nil {
		report.Scraped = true
		report.HitRate = harness.HitRate(before, after)
		report.OriginQPS = harness.OriginQPS(before, after, res.Elapsed)
	}
	return report, nil
}

// String renders the report, including the caveats that make it honest.
func (r BenchReport) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "cachet bench quick — %s, %d rps for %s\n\n", r.Target, r.Rate, r.Measure)
	fmt.Fprintf(&b, "  requests    %d (%.0f rps achieved, %d errors)\n", r.Requests, r.Throughput, r.Errors)
	fmt.Fprintf(&b, "  latency     p50 %s · p90 %s · p99 %s\n",
		round(r.P50), round(r.P90), round(r.P99))

	if r.Scraped {
		fmt.Fprintf(&b, "  hit rate    %.1f%%\n", r.HitRate*100)
		fmt.Fprintf(&b, "  origin      %.0f qps reaching the database\n", r.OriginQPS)
	} else {
		b.WriteString("  hit rate    unavailable — could not scrape the engine's metrics endpoint\n")
	}

	b.WriteString("\n")
	if r.Behind > 0 {
		// Stated first and stated plainly. A reader who takes these numbers away without knowing
		// the generator fell behind has been misled by a tool that knew better.
		fmt.Fprintf(&b, "  ⚠ the generator fell behind on %d requests, so this run measured the\n"+
			"    load generator rather than the system. Lower --rate and try again.\n\n", r.Behind)
	}
	b.WriteString("  This is a smoke test, not a published benchmark: one run, a short window, and\n")
	b.WriteString("  whatever else your machine is doing. It answers \"is this doing anything for me\",\n")
	b.WriteString("  not \"what is the p99\". Origin QPS is the number worth watching — it is the\n")
	b.WriteString("  database load the cache removed.\n")
	return b.String()
}

func round(d time.Duration) time.Duration {
	if d >= time.Millisecond {
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}

// dialTarget normalises a listen address into something gRPC will dial.
func dialTarget(target string) string {
	switch {
	case strings.HasPrefix(target, "unix://"):
		return target
	case strings.HasPrefix(target, "tcp://"):
		return "passthrough:///" + strings.TrimPrefix(target, "tcp://")
	default:
		return "passthrough:///" + target
	}
}
