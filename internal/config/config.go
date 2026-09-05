// Package config loads and validates Cachet's configuration.
//
// One struct, loaded once at boot, validated before anything else starts. The validator is
// deliberately strict and its errors name the offending field: discovering a bad configuration on
// the first user request instead of at startup converts an operator problem into a customer
// problem.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// Config is the complete configuration of a Cachet engine process.
type Config struct {
	// Listen holds one or more addresses, each "tcp://host:port" or "unix:///path".
	//
	// Both transports are supported from the first release rather than TCP first and Unix sockets
	// later, because the sidecar is the default topology and retrofitting the transport would mean
	// re-running every benchmark (ADR 0004, amendment D1).
	Listen []string `koanf:"listen"`

	Shards []Shard `koanf:"shards"`
	Cache  Cache   `koanf:"cache"`

	// DefaultLevel is the consistency level applied when a request does not specify one.
	DefaultLevel string `koanf:"default_level"`

	Consistency   Consistency   `koanf:"consistency"`
	Observability Observability `koanf:"observability"`
	Shutdown      Shutdown      `koanf:"shutdown"`
}

// Shard is one database shard.
type Shard struct {
	ID  string `koanf:"id"`
	DSN string `koanf:"dsn"`
}

// Cache is the cache server this engine talks to. Unused until the cache path lands in Phase 1.
type Cache struct {
	Addresses []string `koanf:"addresses"`
}

// Consistency holds the settings that change what Cachet PROMISES.
//
// These are not tuning knobs. Changing any of them changes a stated guarantee, which is why they
// are logged at boot and exported as gauges (CONSISTENCY.md §9) and why the validator refuses
// nonsensical values instead of clamping them.
type Consistency struct {
	// MaxClockSkew bounds the disagreement between engine and shard clocks. It shortens the
	// BOUNDED(t) window and widens the propagation bound, so the engine stays conservative about
	// its own clock.
	MaxClockSkew time.Duration `koanf:"max_clock_skew"`

	// CDCLagBound is the staleness bound that applies to degraded writes and to writes made
	// directly to MySQL, bypassing Cachet.
	CDCLagBound time.Duration `koanf:"cdc_lag_bound"`

	// WritePathInvalidationBudget is how long synchronous invalidation may take before the write
	// path becomes the suspect in a violation investigation.
	WritePathInvalidationBudget time.Duration `koanf:"write_path_invalidation_budget"`

	// MaxAffectedKeys is where a conditional write stops resolving affected keys exactly and falls
	// back to CDC invalidation, reporting degraded=true.
	MaxAffectedKeys int `koanf:"max_affected_keys"`

	// MaxSessionShards caps the session token's size before it starts evicting watermarks.
	MaxSessionShards int `koanf:"max_session_shards"`

	// EntryTTL is the last-resort safety net and the EVENTUAL convergence bound. Correctness comes
	// from invalidation; the TTL is the backstop, not the strategy.
	EntryTTL time.Duration `koanf:"entry_ttl"`
}

// Observability configures metrics, tracing and logging.
type Observability struct {
	MetricsListen string `koanf:"metrics_listen"`
	LogLevel      string `koanf:"log_level"`
	LogFormat     string `koanf:"log_format"`
	OTLPEndpoint  string `koanf:"otlp_endpoint"`
	ServiceName   string `koanf:"service_name"`
}

// Shutdown configures graceful termination.
type Shutdown struct {
	// DrainTimeout is how long in-flight requests have to finish after the listener stops accepting.
	DrainTimeout time.Duration `koanf:"drain_timeout"`
}

// Default returns the configuration Cachet runs with when nothing is specified.
//
// The defaults must stand on their own — supplying only shard DSNs has to produce a bootable
// engine. A default set that cannot pass its own validator is a trap for the first user, so
// TestDefaultsAreValid asserts exactly that.
func Default() Config {
	return Config{
		Listen:       []string{"tcp://:9090"},
		DefaultLevel: consistency.Session.String(),
		Consistency: Consistency{
			MaxClockSkew:                250 * time.Millisecond,
			CDCLagBound:                 5 * time.Second,
			WritePathInvalidationBudget: 50 * time.Millisecond,
			MaxAffectedKeys:             1000,
			MaxSessionShards:            consistency.DefaultMaxSessionShards,
			EntryTTL:                    4 * time.Hour,
		},
		Observability: Observability{
			MetricsListen: ":9100",
			LogLevel:      "info",
			LogFormat:     "text",
			ServiceName:   "cachet",
		},
		Shutdown: Shutdown{DrainTimeout: 15 * time.Second},
	}
}

// Listener is a parsed listen address.
type Listener struct {
	Network string
	Address string
}

// ParseListen parses a listen address of the form "tcp://host:port" or "unix:///path".
//
// Anything else is an error rather than a best guess. The sidecar topology is the default and rides
// on a Unix socket; silently misreading an address would push a deployment onto the slower path
// with nobody noticing until the latency numbers came in wrong.
func ParseListen(s string) (Listener, error) {
	scheme, rest, ok := strings.Cut(s, "://")
	if !ok || rest == "" {
		return Listener{}, fmt.Errorf("config: listen %q must be tcp://host:port or unix:///path", s)
	}
	switch scheme {
	case "tcp", "tcp4", "tcp6":
		return Listener{Network: scheme, Address: rest}, nil
	case "unix":
		if !strings.HasPrefix(rest, "/") {
			return Listener{}, fmt.Errorf("config: unix listen %q needs an absolute path", s)
		}
		// The kernel's sockaddr_un.sun_path is a fixed-size buffer, so an over-long path fails at
		// bind() with nothing more useful than "invalid argument". Checking it here turns that into
		// a boot error that says what is wrong — which matters because the sidecar topology puts
		// these sockets under generated pod directories that get long.
		if limit := maxUnixSocketPath(); len(rest) > limit {
			return Listener{}, fmt.Errorf(
				"config: unix socket path is %d bytes, over this platform's %d-byte limit: %q",
				len(rest), limit, rest)
		}
		return Listener{Network: "unix", Address: rest}, nil
	default:
		return Listener{}, fmt.Errorf("config: unsupported listen scheme %q in %q", scheme, s)
	}
}

// maxUnixSocketPath returns the platform's sun_path capacity, minus the NUL terminator.
func maxUnixSocketPath() int {
	if runtime.GOOS == "linux" {
		return 107
	}
	return 103 // macOS and the BSDs
}

// Listeners returns the parsed listen addresses. Validate has already checked them.
func (c Config) Listeners() ([]Listener, error) {
	out := make([]Listener, 0, len(c.Listen))
	for _, raw := range c.Listen {
		l, err := ParseListen(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}

// Level returns the parsed default consistency level. Validate has already checked it.
func (c Config) Level() consistency.Level {
	lv, err := consistency.ParseLevel(c.DefaultLevel)
	if err != nil {
		return consistency.Session
	}
	return lv
}

// Validate reports the first problem that would make this configuration unsafe to run.
func (c Config) Validate() error {
	if len(c.Listen) == 0 {
		return errors.New("config: listen must contain at least one address")
	}
	for _, raw := range c.Listen {
		if _, err := ParseListen(raw); err != nil {
			return err
		}
	}

	if len(c.Shards) == 0 {
		return errors.New("config: shards must contain at least one shard")
	}
	seen := make(map[string]struct{}, len(c.Shards))
	for i, sh := range c.Shards {
		if sh.ID == "" {
			return fmt.Errorf("config: shards[%d].id is empty", i)
		}
		if sh.DSN == "" {
			return fmt.Errorf("config: shards[%d] (%s) has no dsn", i, sh.ID)
		}
		if _, dup := seen[sh.ID]; dup {
			// Two shards sharing an id collapse in the router, and half the key space would then
			// route to a database that never received its rows.
			return fmt.Errorf("config: duplicate shard id %q", sh.ID)
		}
		seen[sh.ID] = struct{}{}
	}

	if _, err := consistency.ParseLevel(c.DefaultLevel); err != nil {
		return fmt.Errorf("config: default_level: %w", err)
	}

	if err := c.Consistency.validate(); err != nil {
		return err
	}
	if c.Shutdown.DrainTimeout <= 0 {
		return fmt.Errorf("config: shutdown.drain_timeout must be positive, got %s", c.Shutdown.DrainTimeout)
	}
	return nil
}

func (c Consistency) validate() error {
	if c.MaxClockSkew < 0 {
		return fmt.Errorf("config: consistency.max_clock_skew must not be negative, got %s", c.MaxClockSkew)
	}
	for _, f := range []struct {
		name string
		d    time.Duration
	}{
		{"cdc_lag_bound", c.CDCLagBound},
		{"write_path_invalidation_budget", c.WritePathInvalidationBudget},
		{"entry_ttl", c.EntryTTL},
	} {
		if f.d <= 0 {
			return fmt.Errorf("config: consistency.%s must be positive, got %s", f.name, f.d)
		}
	}
	if c.MaxAffectedKeys <= 0 {
		return fmt.Errorf("config: consistency.max_affected_keys must be positive, got %d", c.MaxAffectedKeys)
	}
	if c.MaxSessionShards <= 0 {
		return fmt.Errorf("config: consistency.max_session_shards must be positive, got %d", c.MaxSessionShards)
	}
	return nil
}

// GuaranteeSettings returns every setting that changes a stated guarantee, for logging at boot and
// export as Prometheus gauges.
//
// Enumerating them in one place is what stops a new guarantee-affecting setting from being added
// without anyone being able to see its value in a running system.
func (c Consistency) GuaranteeSettings() map[string]any {
	return map[string]any{
		"max_clock_skew":                 c.MaxClockSkew,
		"cdc_lag_bound":                  c.CDCLagBound,
		"write_path_invalidation_budget": c.WritePathInvalidationBudget,
		"max_affected_keys":              c.MaxAffectedKeys,
		"max_session_shards":             c.MaxSessionShards,
		"entry_ttl":                      c.EntryTTL,
	}
}

// LogAttrs renders the guarantee settings for the boot log line.
//
// Durations are rendered as "4h0m0s" rather than as nanosecond integers. The line exists so that
// someone reading an incident timeline can see which guarantees were in force; 14400000000000 does
// not serve that purpose, and sorting the keys keeps the line diffable across restarts.
func (c Consistency) LogAttrs() []slog.Attr {
	settings := c.GuaranteeSettings()

	names := make([]string, 0, len(settings))
	for k := range settings {
		names = append(names, k)
	}
	sort.Strings(names)

	out := make([]slog.Attr, 0, len(names))
	for _, k := range names {
		if d, ok := settings[k].(time.Duration); ok {
			out = append(out, slog.String(k, d.String()))
			continue
		}
		out = append(out, slog.Any(k, settings[k]))
	}
	return out
}
