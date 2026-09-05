package config

import (
	"fmt"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// envPrefix is stripped from environment variable names before they are mapped onto config keys.
const envPrefix = "CACHET_"

// envNesting separates nesting levels in an environment variable name, so
// CACHET_CONSISTENCY__MAX_CLOCK_SKEW maps to consistency.max_clock_skew.
//
// Two underscores rather than one, because the config keys contain single underscores themselves;
// a single separator would make max_clock_skew ambiguous with a nested max.clock.skew.
const envNesting = "__"

// Load reads configuration from a YAML file, applies environment overrides, and validates the
// result. It returns the first problem it finds rather than a partially usable config.
//
// Decoding starts from Default() and overwrites only the keys that were actually supplied, so a
// file mentioning one setting cannot silently zero every other one.
//
// env is the environment to read; nil means "no overrides". It is a parameter rather than a read of
// os.Environ so that tests can exercise override precedence without mutating global state and
// racing each other.
func Load(path string, env map[string]string) (Config, error) {
	k := koanf.New(".")

	if path != "" {
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			return Config{}, fmt.Errorf("config: read %s: %w", path, err)
		}
	}
	if err := loadEnv(k, env); err != nil {
		return Config{}, err
	}

	out := Default()
	err := k.UnmarshalWithConf("", &out, koanf.UnmarshalConf{
		Tag: "koanf",
		DecoderConfig: &mapstructure.DecoderConfig{
			Result:           &out,
			WeaklyTypedInput: true,
			// Durations arrive from YAML and the environment as strings ("500ms", "2h"). Without
			// this hook they would decode as zero, and a guarantee-affecting setting would silently
			// become "no bound at all".
			DecodeHook: mapstructure.ComposeDecodeHookFunc(
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.TextUnmarshallerHookFunc(),
			),
		},
	})
	if err != nil {
		return Config{}, fmt.Errorf("config: decode: %w", err)
	}

	if err := out.Validate(); err != nil {
		return Config{}, err
	}
	return out, nil
}

// loadEnv folds CACHET_-prefixed environment variables over whatever the file supplied.
func loadEnv(k *koanf.Koanf, env map[string]string) error {
	values := make(map[string]any, len(env))
	for name, v := range env {
		if !strings.HasPrefix(name, envPrefix) {
			continue
		}
		key := strings.ToLower(strings.TrimPrefix(name, envPrefix))
		values[strings.ReplaceAll(key, envNesting, ".")] = v
	}
	if len(values) == 0 {
		return nil
	}
	if err := k.Load(confmap.Provider(values, "."), nil); err != nil {
		return fmt.Errorf("config: apply environment: %w", err)
	}
	return nil
}
