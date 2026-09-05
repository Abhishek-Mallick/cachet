// Command benchctl runs Cachet's benchmarks, regenerates the README table, and gates regressions.
//
// It exists in Phase 0, before any caching code, because a baseline collected after building the
// thing you want to look good is not a baseline — it is a rationalisation
// (docs/cachet-benchmarking.md §1).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cachetv1 "github.com/Abhishek-Mallick/cachet/api/cachet/v1"
	"github.com/Abhishek-Mallick/cachet/bench/harness"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "benchctl: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: benchctl <run|probe|report|guard> [flags]")
	}
	switch os.Args[1] {
	case "run":
		return runBenchmark(os.Args[2:])
	case "report":
		return report(os.Args[2:])
	case "probe":
		return probeStaleness(os.Args[2:])
	case "guard":
		return guard(os.Args[2:])
	default:
		return fmt.Errorf("unknown command %q (want run, probe, report or guard)", os.Args[1])
	}
}

func runBenchmark(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var (
		workloadPath = fs.String("workload", "bench/workloads/w2.yaml", "workload definition")
		target       = fs.String("target", "tcp://127.0.0.1:9090", "engine address")
		phase        = fs.String("phase", "0-baseline", "phase id recorded in the results file")
		topology     = fs.String("topology", "service", "topology label: service, sidecar or embedded")
		out          = fs.String("out", "bench/results", "results directory")
		runs         = fs.Int("runs", 3, "number of runs; the median is published")
		host         = fs.String("host", "", "environment label, e.g. docker-desktop-macos")
		warmup       = fs.Duration("warmup", 0, "override the workload's warmup")
		measure      = fs.Duration("measure", 0, "override the workload's measure window")
		rate         = fs.Int("rate", 0, "override the workload's rate")
		metricsURL   = fs.String("metrics", "http://127.0.0.1:9100/metrics", "engine metrics endpoint; empty to skip")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	workload, err := harness.LoadWorkload(*workloadPath)
	if err != nil {
		return err
	}
	// Overrides are recorded in the results file rather than applied invisibly, so a short
	// smoke run can never be mistaken for a full one.
	if *warmup > 0 {
		workload.Warmup = *warmup
	}
	if *measure > 0 {
		workload.Measure = *measure
	}
	if *rate > 0 {
		workload.Rate = *rate
	}

	if *host == "" {
		return errors.New("-host is required: a number without its environment is not reproducible " +
			"(docs/cachet-benchmarking.md §4)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, closeConn, err := dial(*target)
	if err != nil {
		return err
	}
	defer closeConn()

	if _, err := client.Handshake(ctx, &cachetv1.HandshakeRequest{
		ProtocolVersion: "cachet.v1",
		ClientVersion:   "benchctl",
	}); err != nil {
		return fmt.Errorf("handshake with %s: %w", *target, err)
	}

	fmt.Printf("workload %s (%s) · %s · rate %d · warmup %s · measure %s · %d runs\n",
		workload.ID, workload.Name, workload.Distribution, workload.Rate,
		workload.Warmup, workload.Measure, *runs)

	metrics := make([]harness.RunMetrics, 0, *runs)
	var cacheAgg harness.CacheMetrics
	var originAgg harness.OriginMetrics

	for i := 1; i <= *runs; i++ {
		// The cache and origin counters are read from the ENGINE, before and after each run. The
		// load generator cannot observe them: it sees latency, while the claim being tested is
		// about database load (benchmarking doc §3.6).
		before, scrapeErr := scrapeIfConfigured(ctx, *metricsURL)
		if scrapeErr != nil {
			return scrapeErr
		}

		start := time.Now()
		m, err := singleRun(ctx, client, workload)
		if err != nil {
			return fmt.Errorf("run %d: %w", i, err)
		}
		window := time.Since(start) - workload.Warmup

		after, scrapeErr := scrapeIfConfigured(ctx, *metricsURL)
		if scrapeErr != nil {
			return scrapeErr
		}

		hitRate := harness.HitRate(before, after)
		originQPS := harness.OriginQPS(before, after, window)
		fmt.Printf("  run %d/%d  %s  throughput %.0f rps  hit %.1f%%  origin %.0f qps  errors %d  behind %d\n",
			i, *runs, m.Read, m.Throughput, hitRate*100, originQPS, m.Errors, m.Behind)

		metrics = append(metrics, m)
		cacheAgg.HitRate += hitRate / float64(*runs)
		originAgg.QPS += originQPS / float64(*runs)
	}

	rep := harness.Report{
		SchemaVersion: harness.SchemaVersion,
		Phase:         *phase,
		Workload:      workload.ID,
		Topology:      *topology,
		Env:           harness.CurrentEnv(*host, images(ctx), commit(ctx)),
		Params:        workload.Params(),
		Runs:          *runs,
		Client:        harness.Aggregate(metrics),
		Cache:         cacheAgg,
		Origin:        originAgg,
	}

	path, err := rep.Save(*out)
	if err != nil {
		return err
	}
	fmt.Printf("\nwrote %s\n", path)
	fmt.Printf("published p99 %s (median of %d runs, spread %s–%s)\n",
		fmtUs(rep.Client.Read.P99), *runs,
		fmtUs(rep.Client.Spread.ReadP99[0]), fmtUs(rep.Client.Spread.ReadP99[1]))
	if !rep.Client.Sufficient {
		// Under-sampling is announced rather than absorbed. Publishing it quietly is how a
		// one-run number ends up in a README next to three-run numbers.
		fmt.Printf("WARNING: %d run(s) is below the three-run minimum; this number is under-sampled\n",
			rep.Client.Runs)
	}
	return nil
}

// singleRun drives one open-loop run against the engine.
func singleRun(ctx context.Context, client cachetv1.CacheServiceClient, w harness.Workload) (harness.RunMetrics, error) {
	gen, err := w.Generator()
	if err != nil {
		return harness.RunMetrics{}, err
	}

	// Reads and writes are chosen from the request ordinal rather than from a random draw, so the
	// read/write interleaving is identical in every run — the same reason the key sequence is
	// seeded (docs/cachet-benchmarking.md §5).
	readsPerCycle := int(w.ReadFraction * 100)

	driver := harness.Driver{
		Rate:    w.Rate,
		Workers: w.Workers,
		Warmup:  w.Warmup,
		Measure: w.Measure,
		Op: func(ctx context.Context, i uint64) error {
			key := harness.KeyFor(gen.Next())
			if int(i%100) < readsPerCycle {
				_, err := client.Get(ctx, &cachetv1.GetRequest{Key: key})
				return err
			}
			_, err := client.Put(ctx, &cachetv1.PutRequest{
				Key:    key,
				Record: &cachetv1.Record{TenantId: 1, Payload: payload()},
			})
			return err
		},
	}

	res, err := driver.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		return harness.RunMetrics{}, err
	}

	return harness.RunMetrics{
		Read:       res.Read.Summarise(),
		Throughput: res.Throughput,
		Errors:     res.Errors,
		Behind:     res.Behind,
	}, nil
}

// scrapeIfConfigured reads the engine's counters, or returns zeros when no endpoint was given.
//
// An unreachable endpoint is an error rather than a silent zero: publishing a 0% hit rate as though
// it had been measured is worse than publishing nothing at all.
func scrapeIfConfigured(ctx context.Context, url string) (harness.Counters, error) {
	if url == "" {
		return harness.Counters{}, nil
	}
	c, err := harness.Scrape(ctx, url)
	if err != nil {
		return harness.Counters{}, fmt.Errorf("%w\n(pass -metrics=\"\" to run without cache and origin measurements)", err)
	}
	return c, nil
}

// payloadBytes matches the fixture's row size so writes do not change the data shape mid-run.
const payloadBytes = 256

func payload() []byte {
	b := make([]byte, payloadBytes)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

// probeStaleness measures how long a committed write stays invisible to a session other than the
// writer's.
//
// This is the metric each phase of the invalidation work is judged on, and the protocol is the one
// in docs/cachet-benchmarking.md §7 reduced to its Phase 1 essentials: a writer advances a sequence
// on a probe key, and a reader holding NO session token polls until it observes that sequence. The
// gap between commit and observation is the staleness.
//
// Using a fresh session for the reader is the whole point. The writer's own session would see its
// write immediately via the watermark, which measures the guarantee that already works rather than
// the one that does not.
func probeStaleness(args []string) error {
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	var (
		target   = fs.String("target", "tcp://127.0.0.1:9090", "engine address")
		phase    = fs.String("phase", "1-ttl", "phase id recorded in the results file")
		out      = fs.String("out", "bench/results", "results directory")
		host     = fs.String("host", "", "environment label")
		samples  = fs.Int("samples", 50, "number of writes to probe")
		interval = fs.Duration("interval", 200*time.Millisecond, "delay between probes")
		giveUp   = fs.Duration("give-up", 45*time.Second, "how long to wait for a write to become visible")
		pollGap  = fs.Duration("poll", 100*time.Millisecond, "delay between reads while waiting")
		ttl      = fs.Duration("ttl", 0, "the engine's configured entry TTL, recorded for context")
		keyBase  = fs.Uint64("key-base", 5000, "first probe key id; MUST exist in the seeded fixture")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *host == "" {
		return errors.New("-host is required: a number without its environment is not reproducible")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, closeConn, err := dial(*target)
	if err != nil {
		return err
	}
	defer closeConn()

	rec := harness.NewRecorder()
	var observations, notConverged int64

	fmt.Printf("probing staleness: %d writes, giving up after %s each\n", *samples, *giveUp)

	for i := 0; i < *samples; i++ {
		if ctx.Err() != nil {
			break
		}
		key := harness.KeyFor(*keyBase + uint64(i%16))
		want := fmt.Sprintf("seq-%d", i)

		// Warm the entry from a reader's point of view first, so the probe measures the time to
		// REPLACE a cached value. Measuring a cold key would time a cache fill, not staleness.
		warm, err := client.Get(ctx, &cachetv1.GetRequest{Key: key})
		if err != nil {
			return fmt.Errorf("warm read %s: %w", key, err)
		}
		// A probe key that does not exist yet caches nothing, because negative caching does not
		// arrive until Phase 2. Every read would then go straight to the database and the probe
		// would report a staleness of one round trip — a number that looks excellent and measures
		// nothing. Refusing to start is the only safe response.
		if !warm.GetFound() {
			return fmt.Errorf(
				"probe key %s does not exist in the fixture; run `make seed` or pass a -key-base "+
					"inside the seeded range (a missing key would silently measure a cache fill "+
					"rather than staleness)", key)
		}

		commit := time.Now()
		if _, err := client.Put(ctx, &cachetv1.PutRequest{
			Key:    key,
			Record: &cachetv1.Record{TenantId: 1, Payload: []byte(want)},
		}); err != nil {
			return fmt.Errorf("probe write %s: %w", key, err)
		}
		observations++

		converged := false
		for time.Since(commit) < *giveUp {
			// No session token: this is a different session from the writer, which is the only way
			// to measure read-OTHERS-writes rather than read-own-writes.
			resp, err := client.Get(ctx, &cachetv1.GetRequest{Key: key})
			if err != nil {
				return fmt.Errorf("probe read %s: %w", key, err)
			}
			if string(resp.GetRecord().GetPayload()) == want {
				rec.Record(time.Since(commit))
				converged = true
				break
			}
			select {
			case <-time.After(*pollGap):
			case <-ctx.Done():
				return nil
			}
		}
		if !converged {
			notConverged++
		}

		select {
		case <-time.After(*interval):
		case <-ctx.Done():
		}
	}

	summary := rec.Summarise()
	rep := harness.Report{
		SchemaVersion: harness.SchemaVersion,
		Phase:         *phase,
		Workload:      "W2",
		Env:           harness.CurrentEnv(*host, images(ctx), commit(ctx)),
		Runs:          1,
		Staleness: harness.StalenessMetrics{
			Observed:     summary,
			Observations: observations,
			NotConverged: notConverged,
			TTLSeconds:   int(ttl.Seconds()),
		},
	}

	fmt.Printf("\nstaleness: %s\n", summary)
	fmt.Printf("observations %d · never converged %d\n", observations, notConverged)
	if notConverged > 0 {
		// Percentiles over only the writes that became visible would be flattering: the ones that
		// did not are the worst cases.
		fmt.Printf("NOTE: the percentiles above EXCLUDE the %d writes that never became visible.\n",
			notConverged)
	}

	path, err := rep.Save(*out)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", path)
	return nil
}

func report(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	in := fs.String("in", "bench/results", "results directory")
	readme := fs.String("readme", "README.md", "README to regenerate")
	if err := fs.Parse(args); err != nil {
		return err
	}

	reports, err := harness.LoadReports(*in)
	if err != nil {
		return err
	}
	if err := harness.UpdateReadme(*readme, reports); err != nil {
		return err
	}

	latest := harness.Latest(reports)
	fmt.Printf("regenerated %s from %d result file(s)\n", *readme, len(reports))
	for _, r := range latest {
		fmt.Printf("  %-14s %-3s p99 %s (%d runs)\n",
			r.Phase, r.Workload, fmtUs(r.Client.Read.P99), r.Client.Runs)
	}
	return nil
}

func guard(args []string) error {
	fs := flag.NewFlagSet("guard", flag.ExitOnError)
	var (
		in        = fs.String("in", "bench/results", "results directory")
		baseline  = fs.String("baseline", "0-baseline", "phase to compare against")
		candidate = fs.String("candidate", "", "phase to check; defaults to the newest non-baseline phase")
		threshold = fs.Float64("threshold", 0.10, "maximum tolerated p99 regression")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	reports := harness.Latest(mustLoad(*in))
	byPhase := make(map[string]harness.Report, len(reports))
	for _, r := range reports {
		byPhase[r.Phase] = r
	}

	base, ok := byPhase[*baseline]
	if !ok {
		// No baseline is not a pass. Treating "nothing to compare against" as success is how a
		// regression gate quietly stops gating.
		return fmt.Errorf("no results for baseline phase %q in %s", *baseline, *in)
	}

	name := *candidate
	if name == "" {
		for _, r := range reports {
			if r.Phase != *baseline {
				name = r.Phase
			}
		}
	}
	if name == "" {
		fmt.Printf("only the baseline is present; nothing to compare\n")
		return nil
	}

	cand, ok := byPhase[name]
	if !ok {
		return fmt.Errorf("no results for phase %q", name)
	}

	change := float64(cand.Client.Read.P99-base.Client.Read.P99) / float64(base.Client.Read.P99)
	fmt.Printf("%s p99 %s vs %s p99 %s  →  %+.1f%%\n",
		cand.Phase, fmtUs(cand.Client.Read.P99), base.Phase, fmtUs(base.Client.Read.P99), change*100)

	if change > *threshold {
		return fmt.Errorf("p99 regressed %.1f%%, over the %.0f%% budget", change*100, *threshold*100)
	}
	return nil
}

func mustLoad(dir string) []harness.Report {
	reports, err := harness.LoadReports(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchctl: %v\n", err)
		os.Exit(1)
	}
	return reports
}

func dial(target string) (cachetv1.CacheServiceClient, func(), error) {
	dialTarget := target
	switch {
	case strings.HasPrefix(target, "unix://"):
		// keep as-is; gRPC understands the unix scheme
	case strings.HasPrefix(target, "tcp://"):
		dialTarget = "passthrough:///" + strings.TrimPrefix(target, "tcp://")
	default:
		return nil, nil, fmt.Errorf("target %q must be tcp://host:port or unix:///path", target)
	}

	conn, err := grpc.NewClient(dialTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", target, err)
	}
	return cachetv1.NewCacheServiceClient(conn), func() { _ = conn.Close() }, nil
}

// images records the container digests a run was measured against.
//
// Pinning tags rather than digests is how `percona-server:8.0` silently changes between two phases
// and produces a regression nobody can explain (docs/cachet-benchmarking.md §4).
func images(ctx context.Context) map[string]string {
	containers := []struct{ name, container string }{
		{"mysql", "cachet-shard0"},
		{"cache", "cachet-cache"},
	}

	out := map[string]string{}
	for _, c := range containers {
		//nolint:gosec // G204: container names are compile-time constants above, never input.
		cmd := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{index .Image}}", c.container)
		digest, err := cmd.Output()
		if err != nil {
			// A missing digest is recorded as absent rather than failing the run, but it means the
			// results file cannot pin what it was measured against.
			continue
		}
		out[c.name] = strings.TrimSpace(string(digest))
	}
	return out
}

func commit(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func fmtUs(us int64) string {
	d := time.Duration(us) * time.Microsecond
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", us)
	}
	return fmt.Sprintf("%.2fms", d.Seconds()*1000)
}
