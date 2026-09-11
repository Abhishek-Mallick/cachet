// Command cachetctl is Cachet's control plane.
//
// Not the product — the thing that makes the product debuggable. Every command here answers a
// question an operator asks during an incident, and each one is deliberately narrow: this tool has
// a manual invalidation in it, and the blast radius of anything it does should be something a person
// can state out loud before they run it.
//
// Commands that would require Sextant (`key trace`) or adaptive admission (`admission explain`) are
// deliberately absent rather than stubbed. A control plane that answers "why was this stale?" with a
// placeholder is worse than one that admits it cannot answer yet.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Abhishek-Mallick/cachet/internal/breaker"
	"github.com/Abhishek-Mallick/cachet/internal/cache"
	"github.com/Abhishek-Mallick/cachet/internal/config"
	"github.com/Abhishek-Mallick/cachet/internal/ctl"
)

var version = "dev"

const usage = `cachetctl — Cachet's control plane

usage: cachetctl <command> [flags]

commands:
  status                     reachability of every cache node
  ring                       both routing rings and each node's share of the key space
  inspect <key>              where a key lives, and what the cache holds for it
  invalidate <key>           tombstone one key by hand (use -dry-run first)
  checkpoint                 each shard's durable CDC tailer position

every command accepts -config <path> and -json.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "cachetctl: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return errors.New("no command given")
	}

	switch args[0] {
	case "status":
		return statusCmd(args[1:])
	case "ring":
		return ringCmd(args[1:])
	case "inspect":
		return inspectCmd(args[1:])
	case "invalidate":
		return invalidateCmd(args[1:])
	case "checkpoint":
		return checkpointCmd(args[1:])
	case "-version", "--version", "version":
		fmt.Println("cachetctl", version)
		return nil
	case "-h", "--help", "help":
		fmt.Print(usage)
		return nil
	default:
		fmt.Print(usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// permute splits args into flags and positional arguments.
//
// Go's flag package stops parsing at the first non-flag argument, so `inspect <key> -config x`
// would treat -config and its value as two more positionals. Every operator writes the subject of
// the command first, and a tool that only accepts `-config x <key>` fails in its most natural usage
// and then blames the key.
//
// A flag that takes a value consumes the next argument, which is why the FlagSet has to be
// consulted rather than pattern-matching on the leading dash: without knowing that -json is boolean
// and -config is not, there is no way to tell whether the token after a flag is its value or the
// key to act on.
func permute(fs *flag.FlagSet, args []string) (flags, positional []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") || a == "-" {
			positional = append(positional, a)
			continue
		}

		flags = append(flags, a)

		// "--" ends flag parsing entirely; everything after it is positional.
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			flags = flags[:len(flags)-1]
			return flags, positional
		}
		// The -name=value form carries its own value.
		name, _, hasValue := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if hasValue {
			continue
		}
		// A boolean flag never consumes the next argument; anything else does.
		if f := fs.Lookup(name); f != nil && !isBoolFlag(f) && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return flags, positional
}

// isBoolFlag reports whether a flag may appear without a value.
func isBoolFlag(f *flag.Flag) bool {
	bf, ok := f.Value.(interface{ IsBoolFlag() bool })
	return ok && bf.IsBoolFlag()
}

// parseArgs parses flags that may appear before or after the positional arguments, and returns the
// positionals.
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	flags, positional := permute(fs, args)
	if err := fs.Parse(flags); err != nil {
		// An unknown flag is an error, never silently reinterpreted as the key to act on. This tool
		// has a manual invalidation in it, and `-drynrun` becoming a positional argument would be a
		// typo that fires the real thing.
		return nil, err
	}
	return positional, nil
}

// commonFlags registers the flags every command shares.
func commonFlags(fs *flag.FlagSet) (configPath *string, asJSON *bool) {
	configPath = fs.String("config", "", "path to a YAML config file")
	asJSON = fs.Bool("json", false, "emit JSON instead of prose")
	return configPath, asJSON
}

// load reads and validates the configuration.
//
// The same loader and the same validator the engine uses, deliberately. A control plane that parsed
// config its own way would eventually disagree with the engine about which shard owns a key, and
// would then send an operator to the wrong node while insisting it was right.
func load(path string) (config.Config, error) {
	cfg, err := config.Load(path, envMap())
	if err != nil {
		return config.Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return config.Config{}, err
	}
	return cfg, nil
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

// emit prints a report as prose or JSON.
func emit(v fmt.Stringer, asJSON bool) error {
	if !asJSON {
		fmt.Print(v.String())
		return nil
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("render json: %w", err)
	}
	fmt.Println(string(b))
	return nil
}

func statusCmd(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	configPath, asJSON := commonFlags(fs)
	timeout := fs.Duration("timeout", 2*time.Second, "per-node probe timeout")
	if _, err := parseArgs(fs, args); err != nil {
		return err
	}

	cfg, err := load(*configPath)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	report, err := ctl.Health(ctx, cfg, *timeout)
	if err != nil {
		return err
	}
	return emit(report, *asJSON)
}

func ringCmd(args []string) error {
	fs := flag.NewFlagSet("ring", flag.ContinueOnError)
	configPath, asJSON := commonFlags(fs)
	sample := fs.Int("sample", 30000, "how many keys to sample when measuring each node's share")
	if _, err := parseArgs(fs, args); err != nil {
		return err
	}

	cfg, err := load(*configPath)
	if err != nil {
		return err
	}

	report, err := ctl.Ring(cfg, *sample)
	if err != nil {
		return err
	}
	return emit(report, *asJSON)
}

func inspectCmd(args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	configPath, asJSON := commonFlags(fs)
	rest, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(rest) != 1 {
		return errors.New("usage: cachetctl inspect <key>")
	}

	cfg, err := load(*configPath)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Without a cache there is nothing to inspect, and routing alone is what `ring` is for. Saying
	// so beats printing a report whose every interesting field is empty.
	if len(cfg.Cache.Addresses) == 0 {
		return errors.New("no cache configured; there is nothing to inspect")
	}

	client, err := openCache(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	report, err := ctl.Inspect(ctx, client, cfg, rest[0])
	if err != nil {
		return err
	}
	return emit(report, *asJSON)
}

func invalidateCmd(args []string) error {
	fs := flag.NewFlagSet("invalidate", flag.ContinueOnError)
	configPath, asJSON := commonFlags(fs)
	dryRun := fs.Bool("dry-run", false, "show what would happen without changing anything")
	rest, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(rest) != 1 {
		return errors.New("usage: cachetctl invalidate <key> [-dry-run]")
	}

	cfg, err := load(*configPath)
	if err != nil {
		return err
	}
	if len(cfg.Cache.Addresses) == 0 {
		return errors.New("no cache configured; there is nothing to invalidate")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := openCache(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	report, err := ctl.Invalidate(ctx, client, cfg, rest[0], *dryRun)
	if err != nil {
		return err
	}
	return emit(report, *asJSON)
}

func checkpointCmd(args []string) error {
	fs := flag.NewFlagSet("checkpoint", flag.ContinueOnError)
	configPath, asJSON := commonFlags(fs)
	stateDir := fs.String("state-dir", "./.flux", "flux's checkpoint directory")
	if _, err := parseArgs(fs, args); err != nil {
		return err
	}

	cfg, err := load(*configPath)
	if err != nil {
		return err
	}

	report, err := ctl.Checkpoints(cfg, *stateDir)
	if err != nil {
		return err
	}
	return emit(report, *asJSON)
}

// openCache connects to the configured cache ring.
func openCache(ctx context.Context, cfg config.Config) (*cache.Client, error) {
	return cache.New(ctx, cache.Options{
		Addresses: cfg.Cache.Addresses,
		TTL:       cfg.Consistency.EntryTTL,
		Breaker: breaker.Options{
			Window:       cfg.Cache.Breaker.Window,
			Buckets:      cfg.Cache.Breaker.Buckets,
			MinRequests:  cfg.Cache.Breaker.MinRequests,
			FailureFloor: cfg.Cache.Breaker.FailureFloor,
			MaxShed:      cfg.Cache.Breaker.MaxShed,
		},
	})
}
