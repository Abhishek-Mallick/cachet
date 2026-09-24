package sextant

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Sextant's own configuration.
//
// Its own, deliberately. Sextant is pointed at deployments that have never heard of Cachet, and a
// verifier that demanded a cachet.yaml to run would be a verifier for one product rather than a
// tool for the problem. It also means Sextant versions on its own track: a change to Cachet's
// config format is not a reason to re-learn this one.
//
// Everything here is checkable at load, and everything that cannot be checked at load is checked
// at connect. The alternative — a verifier that starts, samples nothing and reports a clean number
// — is the single most damaging way this component can fail.

// Config is a sextant.yaml.
type Config struct {
	// Tier is how entries are compared: value, version or cachet. It appears on every metric.
	Tier string `yaml:"tier"`

	Origin   OriginConfig   `yaml:"origin"`
	Cache    CacheConfig    `yaml:"cache"`
	Keys     KeysConfig     `yaml:"keys"`
	Bound    BoundConfig    `yaml:"bound"`
	Sampling SamplingConfig `yaml:"sampling"`

	// SLOWindow is the rolling window the published figure is measured over.
	SLOWindow time.Duration `yaml:"slo_window"`

	// MetricsListen is where the Prometheus endpoint binds.
	MetricsListen string `yaml:"metrics_listen"`

	// Shadow marks this as observing a deployment that serves no application traffic. It changes
	// nothing about the arithmetic and everything about how the result should be read.
	Shadow bool `yaml:"shadow"`
}

// OriginConfig describes the system of record.
type OriginConfig struct {
	DSNs          []string `yaml:"dsns"`
	Table         string   `yaml:"table"`
	KeyColumn     string   `yaml:"key_column"`
	VersionColumn string   `yaml:"version_column"`
	ValueColumns  []string `yaml:"value_columns"`

	// Shard is "single" or "hash". Declared rather than inferred, because it is only correct if it
	// is the same function the application uses — a verifier that guessed would read the wrong
	// writer and find every key missing, which looks like a healthy cache.
	Shard string `yaml:"shard"`
}

// CacheConfig describes the cache being verified.
type CacheConfig struct {
	Addrs    []string `yaml:"addrs"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	DB       int      `yaml:"db"`

	// KeyTemplate maps a key onto the application's cache key. `{key}` is replaced.
	KeyTemplate string `yaml:"key_template"`

	Codec        string   `yaml:"codec"`
	VersionField string   `yaml:"version_field"`
	ValueFields  []string `yaml:"value_fields"`
}

// KeysConfig describes where the keys to check come from.
type KeysConfig struct {
	// Source is "binlog", "file" or "scan". Binlog is primary; scan is degraded and says so.
	Source string `yaml:"source"`

	// File is the key list, for the file source.
	File string `yaml:"file"`

	// Capacity bounds the binlog source's buffer. It runs beside a live system.
	Capacity int `yaml:"capacity"`
}

// BoundConfig is the propagation bound, summed from what the system promises.
type BoundConfig struct {
	WritePathInvalidationBudget time.Duration `yaml:"write_path_invalidation_budget"`
	CDCLagBound                 time.Duration `yaml:"cdc_lag_bound"`
	MaxClockSkew                time.Duration `yaml:"max_clock_skew"`
}

// SamplingConfig is the load Sextant puts on the system it observes.
type SamplingConfig struct {
	Interval time.Duration `yaml:"interval"`
	Batch    int           `yaml:"batch"`
}

// DefaultConfig is what a sextant.yaml starts from.
func DefaultConfig() Config {
	return Config{
		Tier:  string(TierVersion),
		Keys:  KeysConfig{Source: string(SourceBinlog), Capacity: 10_000},
		Cache: CacheConfig{Codec: string(CodecRaw)},
		Bound: BoundConfig{
			// Not zero. A zero bound reports every in-flight invalidation as a violation, the
			// number becomes noise within a day, and nobody opens the dashboard again.
			WritePathInvalidationBudget: 50 * time.Millisecond,
			CDCLagBound:                 5 * time.Second,
			MaxClockSkew:                250 * time.Millisecond,
		},
		Sampling:      SamplingConfig{Interval: time.Second, Batch: 100},
		SLOWindow:     5 * time.Minute,
		MetricsListen: ":9101",
	}
}

// LoadConfig reads and validates a sextant.yaml.
func LoadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // the path is the operator's own argument
	if err != nil {
		return Config{}, fmt.Errorf("sextant: read %s: %w", path, err)
	}

	// Decoding onto the defaults, so a file mentioning one setting cannot silently zero every
	// other one — including the propagation bound, where a zero means "report everything".
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("sextant: parse %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks everything that can be checked without connecting.
func (c Config) Validate() error {
	tier, err := ParseTier(c.Tier)
	if err != nil {
		return err
	}
	codec, err := ParseCodec(c.Cache.Codec)
	if err != nil {
		return err
	}

	switch {
	case len(c.Origin.DSNs) == 0:
		return errors.New("sextant: origin.dsns is empty; there is nothing to compare the cache against")
	case c.Origin.Table == "":
		return errors.New("sextant: origin.table is empty")
	case c.Origin.KeyColumn == "":
		return errors.New("sextant: origin.key_column is empty")
	case len(c.Cache.Addrs) == 0:
		return errors.New("sextant: cache.addrs is empty")
	case c.Cache.KeyTemplate == "":
		return errors.New("sextant: cache.key_template is empty")
	}

	// The tier and the adapters have to agree, or the run starts and measures something other than
	// what its metrics will claim.
	if tier.ComparesVersions() {
		if c.Origin.VersionColumn == "" {
			return fmt.Errorf("sextant: tier %q compares versions, so origin.version_column is required", tier)
		}
		if codec == CodecRaw {
			return fmt.Errorf(
				"sextant: tier %q compares versions, but cache.codec is %q, which cannot extract one. "+
					"Use codec json or hash, or tier value", tier, codec)
		}
	} else {
		if len(c.Origin.ValueColumns) == 0 {
			return fmt.Errorf("sextant: tier %q compares values, so origin.value_columns is required", tier)
		}
	}

	switch c.Keys.Source {
	case string(SourceBinlog), string(SourceScan):
	case string(SourceFile):
		if c.Keys.File == "" {
			return errors.New("sextant: keys.source is file, so keys.file is required")
		}
	default:
		return fmt.Errorf("sextant: unknown keys.source %q; want binlog, file or scan", c.Keys.Source)
	}

	switch c.Origin.Shard {
	case "", "single", "hash":
	default:
		return fmt.Errorf("sextant: unknown origin.shard %q; want single or hash", c.Origin.Shard)
	}
	if c.Origin.Shard == "hash" && len(c.Origin.DSNs) < 2 {
		return errors.New("sextant: origin.shard is hash with a single writer; use single")
	}

	if c.Bound.WritePathInvalidationBudget+c.Bound.CDCLagBound+c.Bound.MaxClockSkew <= 0 {
		return errors.New(
			"sextant: the propagation bound sums to zero, which reports every in-flight " +
				"invalidation as a violation")
	}
	if c.Sampling.Interval <= 0 || c.Sampling.Batch <= 0 {
		return errors.New("sextant: sampling.interval and sampling.batch must be positive")
	}
	if c.SLOWindow <= 0 {
		return errors.New("sextant: slo_window must be positive")
	}
	return nil
}

// Built is a verifier and the resources it holds.
type Built struct {
	Verifier *Verifier
	SLO      *SLO
	Tracer   *Tracer

	// Keys is the source, exposed so a caller can feed it — a binlog tailer calls Touch, a scan
	// source needs Fill driving.
	Keys KeySource

	origin *SQLOrigin
	cache  *RedisCache
}

// Close releases the connections.
func (b *Built) Close() error {
	var err error
	if b.cache != nil {
		err = b.cache.Close()
	}
	if b.origin != nil {
		if oerr := b.origin.Close(); oerr != nil && err == nil {
			err = oerr
		}
	}
	return err
}

// Build constructs a verifier from a configuration, connecting to both sides.
//
// Connecting here rather than lazily: a verifier that could not reach the cache would otherwise
// start, sample nothing, and publish a window with zero observations — which the SLO reports as
// "not known" but which an operator glancing at a dashboard reads as fine.
func Build(ctx context.Context, cfg Config) (*Built, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	tier, _ := ParseTier(cfg.Tier)
	codec, _ := ParseCodec(cfg.Cache.Codec)

	var shardFor func(string) int
	if cfg.Origin.Shard == "hash" {
		shardFor = HashShard(len(cfg.Origin.DSNs))
	}

	origin, err := NewSQLOrigin(ctx, SQLOriginOptions{
		DSNs:          cfg.Origin.DSNs,
		Table:         cfg.Origin.Table,
		KeyColumn:     cfg.Origin.KeyColumn,
		VersionColumn: cfg.Origin.VersionColumn,
		ValueColumns:  cfg.Origin.ValueColumns,
		ShardFor:      shardFor,
	})
	if err != nil {
		return nil, err
	}

	cache, err := NewRedisCache(ctx, RedisCacheOptions{
		Addrs:        cfg.Cache.Addrs,
		Username:     cfg.Cache.Username,
		Password:     cfg.Cache.Password,
		DB:           cfg.Cache.DB,
		KeyTemplate:  cfg.Cache.KeyTemplate,
		Codec:        codec,
		VersionField: cfg.Cache.VersionField,
		ValueFields:  cfg.Cache.ValueFields,
	})
	if err != nil {
		_ = origin.Close()
		return nil, err
	}

	keys, err := buildKeySource(cfg)
	if err != nil {
		_ = cache.Close()
		_ = origin.Close()
		return nil, err
	}

	slo := NewSLO(cfg.SLOWindow)
	tracer := NewTracer(TracerOptions{})
	verifier, err := NewVerifier(VerifierOptions{
		Cache:    cache,
		Origin:   origin,
		Keys:     keys,
		Tier:     tier,
		Bound:    NewPropagationBound(cfg.Bound.WritePathInvalidationBudget, cfg.Bound.CDCLagBound, cfg.Bound.MaxClockSkew),
		SLO:      slo,
		Tracer:   tracer,
		Interval: cfg.Sampling.Interval,
		Batch:    cfg.Sampling.Batch,
		Shadow:   cfg.Shadow,
	})
	if err != nil {
		_ = cache.Close()
		_ = origin.Close()
		return nil, err
	}

	return &Built{
		Verifier: verifier, SLO: slo, Tracer: tracer, Keys: keys,
		origin: origin, cache: cache,
	}, nil
}

func buildKeySource(cfg Config) (KeySource, error) {
	switch cfg.Keys.Source {
	case string(SourceFile):
		return NewFileKeys(cfg.Keys.File)
	case string(SourceScan):
		// Built by the caller, which owns the Redis client the scan runs on. Returning an error
		// here rather than a half-built source keeps the degraded path explicit.
		return nil, errors.New(
			"sextant: keys.source scan must be built by the caller with NewScanKeys, " +
				"so the client it scans is the one it was given")
	default:
		capacity := cfg.Keys.Capacity
		if capacity <= 0 {
			capacity = 10_000
		}
		return NewRecentKeys(capacity), nil
	}
}
