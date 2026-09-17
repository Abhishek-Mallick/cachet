// Package ctl implements the operations behind cachetctl.
//
// The logic lives here rather than in cmd/cachetctl so it can be tested without a terminal. An
// operator tool whose behaviour is only exercised by running it by hand is a tool that quietly
// stops being correct, which is a poor property for the thing people reach for during an incident.
//
// Every report renders two ways: String() for a human reading it at 3am, and JSON tags for the
// runbook, alert or CI check that has to parse it. A tool that only prints a table forces every one
// of those callers to grep.
package ctl

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Abhishek-Mallick/cachet/internal/admission"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/cdc"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/engine"
	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// sampleKey is the key pattern used to estimate ring distribution. It matches the benchmark
// fixtures, so the shares reported here are the shares the benchmarks actually see.
func sampleKey(i int) string { return "entities:" + strconv.Itoa(i) }

// RingReport describes both of Cachet's routing rings.
type RingReport struct {
	CacheNodes []string `json:"cache_nodes"`
	Shards     []string `json:"shards"`

	// CacheShare and ShardShare are each node's measured share of a sampled key space. They are
	// reported side by side because the two rings are independent, and seeing them together is what
	// stops an operator assuming a cache node and a shard are the same thing under two names.
	CacheShare map[string]float64 `json:"cache_share,omitempty"`
	ShardShare map[string]float64 `json:"shard_share"`

	// Sampled is how many keys the shares were measured over, so a reader can judge the precision
	// rather than trusting a percentage with no denominator.
	Sampled int `json:"sampled"`
}

// Ring reports the cache ring and the shard ring, with each node's share of a sampled key space.
func Ring(cfg config.Config, sample int) (RingReport, error) {
	if sample <= 0 {
		return RingReport{}, fmt.Errorf("ctl: sample size must be positive, got %d", sample)
	}

	shardIDs := make([]storage.ShardID, 0, len(cfg.Shards))
	for _, s := range cfg.Shards {
		shardIDs = append(shardIDs, storage.ShardID(s.ID))
	}
	shards, err := storage.NewRouter(shardIDs)
	if err != nil {
		return RingReport{}, fmt.Errorf("ctl: shard ring: %w", err)
	}

	r := RingReport{
		Shards:     make([]string, 0, len(shardIDs)),
		ShardShare: make(map[string]float64, len(shardIDs)),
		Sampled:    sample,
	}
	for _, s := range shards.Shards() {
		r.Shards = append(r.Shards, string(s))
	}
	sort.Strings(r.Shards)

	// An uncached configuration is supported, not an error: it is the baseline every benchmark row
	// is compared against.
	var cacheRouter *cache.Router
	if len(cfg.Cache.Addresses) > 0 {
		cacheRouter, err = cache.NewRouter(cfg.Cache.Addresses)
		if err != nil {
			return RingReport{}, fmt.Errorf("ctl: cache ring: %w", err)
		}
		r.CacheNodes = cacheRouter.Nodes()
		r.CacheShare = make(map[string]float64, len(r.CacheNodes))
	}

	cacheCounts := make(map[string]int, len(r.CacheNodes))
	shardCounts := make(map[string]int, len(r.Shards))
	for i := 0; i < sample; i++ {
		key := sampleKey(i)

		shard, err := shards.ShardFor(key)
		if err != nil {
			return RingReport{}, fmt.Errorf("ctl: route %s: %w", key, err)
		}
		shardCounts[string(shard)]++

		if cacheRouter != nil {
			node, err := cacheRouter.NodeFor(key)
			if err != nil {
				return RingReport{}, fmt.Errorf("ctl: route %s: %w", key, err)
			}
			cacheCounts[node]++
		}
	}

	for _, s := range r.Shards {
		r.ShardShare[s] = float64(shardCounts[s]) / float64(sample)
	}
	for _, n := range r.CacheNodes {
		r.CacheShare[n] = float64(cacheCounts[n]) / float64(sample)
	}
	return r, nil
}

// String renders the report for a human.
func (r RingReport) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "cache ring (%d node(s))\n", len(r.CacheNodes))
	if len(r.CacheNodes) == 0 {
		b.WriteString("  (none configured — every read reaches the database)\n")
	}
	for _, n := range r.CacheNodes {
		fmt.Fprintf(&b, "  %-28s %5.1f%% of keys\n", n, r.CacheShare[n]*100)
	}

	fmt.Fprintf(&b, "\nshard ring (%d shard(s))\n", len(r.Shards))
	for _, s := range r.Shards {
		fmt.Fprintf(&b, "  %-28s %5.1f%% of keys\n", s, r.ShardShare[s]*100)
	}

	fmt.Fprintf(&b, "\nshares measured over %d sampled keys; the two rings are independent by design\n", r.Sampled)
	return b.String()
}

// KeyLocation says where one key lives.
//
// Two answers, not one, because the cache ring and the shard ring are independent. Reporting only
// the shard is precisely what leads people to believe the cache is sharded alongside it.
type KeyLocation struct {
	Key string `json:"key"`

	// CacheNode is empty when no cache is configured.
	CacheNode string `json:"cache_node,omitempty"`
	Shard     string `json:"shard"`
}

// Locate reports which cache node and which shard own a key.
func Locate(cfg config.Config, key string) (KeyLocation, error) {
	// Parsed with the engine's own parser, so the tool cannot disagree with the engine about what a
	// key means. A key this tool half-understands would route somewhere deterministic and send an
	// operator to inspect a node that never held the row.
	parsed, err := engine.ParseKey(key)
	if err != nil {
		return KeyLocation{}, fmt.Errorf("ctl: %w", err)
	}
	canonical := parsed.String()

	shardIDs := make([]storage.ShardID, 0, len(cfg.Shards))
	for _, s := range cfg.Shards {
		shardIDs = append(shardIDs, storage.ShardID(s.ID))
	}
	shards, err := storage.NewRouter(shardIDs)
	if err != nil {
		return KeyLocation{}, fmt.Errorf("ctl: shard ring: %w", err)
	}
	shard, err := shards.ShardFor(canonical)
	if err != nil {
		return KeyLocation{}, fmt.Errorf("ctl: route %s: %w", canonical, err)
	}

	loc := KeyLocation{Key: canonical, Shard: string(shard)}

	if len(cfg.Cache.Addresses) > 0 {
		cacheRouter, err := cache.NewRouter(cfg.Cache.Addresses)
		if err != nil {
			return KeyLocation{}, fmt.Errorf("ctl: cache ring: %w", err)
		}
		node, err := cacheRouter.NodeFor(canonical)
		if err != nil {
			return KeyLocation{}, fmt.Errorf("ctl: route %s: %w", canonical, err)
		}
		loc.CacheNode = node
	}
	return loc, nil
}

// String renders the location for a human.
func (l KeyLocation) String() string {
	node := l.CacheNode
	if node == "" {
		node = "(no cache configured)"
	}
	return fmt.Sprintf("key         %s\ncache node  %s\nshard       %s\n", l.Key, node, l.Shard)
}

// ShardCheckpoint is one shard's tailer position.
type ShardCheckpoint struct {
	Shard    string       `json:"shard"`
	Position cdc.Position `json:"position"`

	// Present distinguishes "the tailer has never committed a position" from "the tailer is at the
	// zero position". They are different incidents and the second one does not exist.
	Present bool   `json:"present"`
	Path    string `json:"path"`
}

// CheckpointReport is the tailer position of every configured shard.
type CheckpointReport struct {
	StateDir string            `json:"state_dir"`
	Shards   []ShardCheckpoint `json:"shards"`
}

// Checkpoints reads the durable tailer position for every configured shard.
//
// The report is driven by the CONFIGURED shards rather than by whatever files happen to be in the
// directory. A shard with no checkpoint is a row that says so, because "the tailer for shard2 was
// never started" is exactly the incident this command exists to make visible — and a stray file
// must not become a phantom shard.
func Checkpoints(cfg config.Config, stateDir string) (CheckpointReport, error) {
	if err := dirExists(stateDir); err != nil {
		// A typo'd --state-dir must not read as "no tailer has ever run". That mistake sends an
		// operator hunting a CDC outage that is not happening.
		return CheckpointReport{}, err
	}

	r := CheckpointReport{StateDir: stateDir, Shards: make([]ShardCheckpoint, 0, len(cfg.Shards))}
	for _, sh := range cfg.Shards {
		path := filepath.Join(stateDir, sh.ID+".pos")
		pos, ok, err := cdc.NewFileCheckpoint(path).Load()
		if err != nil {
			return CheckpointReport{}, fmt.Errorf("ctl: checkpoint for %s: %w", sh.ID, err)
		}
		r.Shards = append(r.Shards, ShardCheckpoint{
			Shard: sh.ID, Position: pos, Present: ok, Path: path,
		})
	}
	return r, nil
}

// String renders the checkpoints for a human.
func (r CheckpointReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "tailer checkpoints in %s\n", r.StateDir)
	for _, s := range r.Shards {
		if !s.Present {
			fmt.Fprintf(&b, "  %-12s no checkpoint — this shard's tailer has never committed a position\n", s.Shard)
			continue
		}
		fmt.Fprintf(&b, "  %-12s %s\n", s.Shard, s.Position)
	}
	return b.String()
}

// dirExists reports whether path is a directory, with an error naming it if not.
func dirExists(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("ctl: state dir %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("ctl: state dir %s is not a directory", path)
	}
	return nil
}

// EntryView is what the cache currently holds for a key.
type EntryView struct {
	// Both versions are reported, never one. They answer different questions — RowVersion orders
	// fills against each other, FillVersion answers freshness — and an operator shown only one of
	// them cannot tell a stale entry from an old row that was read a millisecond ago
	// (CONSISTENCY.md §1).
	RowVersion  uint64 `json:"row_version"`
	FillVersion uint64 `json:"fill_version"`

	// Negative marks a cached absence. A negative entry and an absent one look identical to a
	// reader and mean completely different things to an operator: "we know this row does not exist"
	// versus "we have not looked".
	Negative bool `json:"negative"`

	PayloadBytes int `json:"payload_bytes"`
}

// KeyInspection is the answer to "what do we know about this key?".
type KeyInspection struct {
	KeyLocation

	Cached bool          `json:"cached"`
	TTL    time.Duration `json:"ttl,omitempty"`
	Entry  *EntryView    `json:"entry,omitempty"`
}

// Inspect reports where a key lives and what the cache currently holds for it.
func Inspect(ctx context.Context, c *cache.Client, cfg config.Config, key string) (KeyInspection, error) {
	loc, err := Locate(cfg, key)
	if err != nil {
		return KeyInspection{}, err
	}
	insp := KeyInspection{KeyLocation: loc}

	entry, hit, err := c.Get(ctx, loc.Key)
	if err != nil {
		return KeyInspection{}, fmt.Errorf("ctl: inspect %s: %w", loc.Key, err)
	}
	if !hit {
		// Routing is still returned. "Not cached" is only actionable next to "and it would live
		// here", which is where the operator looks next.
		return insp, nil
	}

	insp.Cached = true
	insp.Entry = &EntryView{
		RowVersion:   entry.RowVersion,
		FillVersion:  entry.FillVersion,
		Negative:     entry.Negative,
		PayloadBytes: len(entry.Payload),
	}

	ttl, present, err := c.RemainingTTL(ctx, loc.Key)
	if err != nil {
		return KeyInspection{}, fmt.Errorf("ctl: inspect %s: %w", loc.Key, err)
	}
	if present {
		insp.TTL = ttl
	}
	return insp, nil
}

// String renders the inspection for a human.
func (k KeyInspection) String() string {
	var b strings.Builder
	b.WriteString(k.KeyLocation.String())

	if !k.Cached {
		b.WriteString("cached      no\n")
		return b.String()
	}

	kind := "value"
	if k.Entry.Negative {
		kind = "negative (a cached absence)"
	}
	fmt.Fprintf(&b, "cached      yes — %s\n", kind)
	fmt.Fprintf(&b, "row ver     %d   (orders fills against each other)\n", k.Entry.RowVersion)
	fmt.Fprintf(&b, "fill ver    %d   (answers freshness)\n", k.Entry.FillVersion)
	fmt.Fprintf(&b, "payload     %d bytes\n", k.Entry.PayloadBytes)
	fmt.Fprintf(&b, "expires in  %s\n", k.TTL.Round(time.Second))
	return b.String()
}

// InvalidateResult records what a manual invalidation did.
type InvalidateResult struct {
	Key       string `json:"key"`
	CacheNode string `json:"cache_node"`
	Shard     string `json:"shard"`

	// Version is the tombstone's version. It is stamped from the current clock so the marker beats
	// anything already in flight: a tombstone with a low version would lose its compare-and-set to a
	// read that started before the operator ran the command, and the entry they just "removed" would
	// quietly come back.
	Version uint64 `json:"version"`

	Applied bool `json:"applied"`
	DryRun  bool `json:"dry_run"`
}

// Invalidate tombstones a key by hand.
//
// This is the operator escape hatch, not a routine path. It is deliberately one key at a time: the
// blast radius of a manual invalidation should be something a person can state out loud before they
// run it.
func Invalidate(ctx context.Context, c *cache.Client, cfg config.Config, key string, dryRun bool) (InvalidateResult, error) {
	loc, err := Locate(cfg, key)
	if err != nil {
		return InvalidateResult{}, err
	}

	version := storage.NewClock(time.Now).Next()
	res := InvalidateResult{
		Key:       loc.Key,
		CacheNode: loc.CacheNode,
		Shard:     loc.Shard,
		Version:   uint64(version),
		DryRun:    dryRun,
	}
	if dryRun {
		// A blast-radius preview that actually fires is worse than no preview at all.
		return res, nil
	}

	applied, err := c.Tombstone(ctx, loc.Key, uint64(version))
	if err != nil {
		return InvalidateResult{}, fmt.Errorf("ctl: invalidate %s: %w", loc.Key, err)
	}
	res.Applied = applied
	return res, nil
}

// String renders the result for a human.
func (r InvalidateResult) String() string {
	verb := "invalidated"
	switch {
	case r.DryRun:
		verb = "WOULD invalidate (dry run; nothing was changed)"
	case !r.Applied:
		verb = "NOT invalidated — a newer version already holds the entry"
	}
	return fmt.Sprintf("%s %s\n  cache node  %s\n  shard       %s\n  version     %d\n",
		verb, r.Key, r.CacheNode, r.Shard, r.Version)
}

// AdmissionExplanation is why a key is or is not being cached.
type AdmissionExplanation struct {
	Key   string  `json:"key"`
	Admit bool    `json:"admit"`
	Ratio float64 `json:"read_write_ratio"`

	Reads  uint32 `json:"reads"`
	Writes uint32 `json:"writes"`

	Reason string `json:"reason"`
}

// ExplainAdmission reports the admission decision for a key.
//
// The command exists because a cache that silently declines to cache a key is indistinguishable
// from a cache that is broken, and "why is this key not cached?" is the first question anyone asks.
// A control plane that cannot answer it leaves an operator to guess, and they will guess that
// something is wrong.
func ExplainAdmission(c *admission.Controller, key string) (AdmissionExplanation, error) {
	parsed, err := engine.ParseKey(key)
	if err != nil {
		return AdmissionExplanation{}, fmt.Errorf("ctl: %w", err)
	}
	canonical := parsed.String()

	d := c.Explain(canonical)
	reads, writes := c.Counts(canonical)
	return AdmissionExplanation{
		Key: canonical, Admit: d.Admit, Ratio: d.Ratio,
		Reads: reads, Writes: writes, Reason: d.Reason,
	}, nil
}

// String renders the explanation for a human.
func (e AdmissionExplanation) String() string {
	verdict := "NOT cached"
	if e.Admit {
		verdict = "cached"
	}
	ratio := fmt.Sprintf("%.1f:1", e.Ratio)
	if math.IsInf(e.Ratio, 1) {
		ratio = "read-only"
	}
	return fmt.Sprintf("key         %s\nadmission   %s\nread:write  %s (%d reads, %d writes)\nreason      %s\n",
		e.Key, verdict, ratio, e.Reads, e.Writes, e.Reason)
}

// NodeHealth is one cache node's reachability.
type NodeHealth struct {
	Address string        `json:"address"`
	Healthy bool          `json:"healthy"`
	Latency time.Duration `json:"latency,omitempty"`
	Error   string        `json:"error,omitempty"`
}

// HealthReport is the reachability of every configured cache node.
type HealthReport struct {
	CacheNodes []NodeHealth `json:"cache_nodes"`
}

// Health probes every cache node and reports what answered.
//
// It never returns an error for an unreachable node. A health command that gives up on the first
// dead node cannot report on the rest of the ring, which is the moment an operator most needs the
// whole picture.
func Health(ctx context.Context, cfg config.Config, timeout time.Duration) (HealthReport, error) {
	if len(cfg.Cache.Addresses) == 0 {
		return HealthReport{}, nil
	}

	router, err := cache.NewRouter(cfg.Cache.Addresses)
	if err != nil {
		return HealthReport{}, fmt.Errorf("ctl: cache ring: %w", err)
	}

	nodes := router.Nodes()
	report := HealthReport{CacheNodes: make([]NodeHealth, 0, len(nodes))}
	for _, addr := range nodes {
		report.CacheNodes = append(report.CacheNodes, probe(ctx, addr, timeout))
	}
	return report, nil
}

// probe pings one node and times the round trip.
func probe(ctx context.Context, addr string, timeout time.Duration) NodeHealth {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  timeout,
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	})
	defer func() { _ = rdb.Close() }()

	start := time.Now()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return NodeHealth{Address: addr, Healthy: false, Error: err.Error()}
	}
	return NodeHealth{Address: addr, Healthy: true, Latency: time.Since(start)}
}

// String renders the health report for a human.
func (h HealthReport) String() string {
	if len(h.CacheNodes) == 0 {
		return "no cache configured — every read reaches the database\n"
	}

	var b strings.Builder
	healthy := 0
	for _, n := range h.CacheNodes {
		if n.Healthy {
			healthy++
		}
	}
	fmt.Fprintf(&b, "cache ring: %d/%d node(s) answering\n", healthy, len(h.CacheNodes))
	for _, n := range h.CacheNodes {
		if n.Healthy {
			fmt.Fprintf(&b, "  %-28s ok    %s\n", n.Address, n.Latency.Round(time.Microsecond))
			continue
		}
		fmt.Fprintf(&b, "  %-28s DOWN  %s\n", n.Address, n.Error)
	}
	return b.String()
}
