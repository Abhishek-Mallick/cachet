package config

import (
	"fmt"
	"regexp"
	"strings"
)

// Keeping credentials out of the file that holds the configuration.
//
// A DSN carries a username and a password. A config file carrying one is a config file that cannot
// be committed, cannot go in a ConfigMap, and shows up in `helm get values` — which is why the Helm
// chart takes credentials from a Secret and mounts them as environment variables.
//
// Those two facts only meet if a DSN can name an environment variable. `${CACHET_DB_PASSWORD}` is
// that, and the narrowness is deliberate: only `${NAME}`, only in DSNs, and an unset variable is an
// error rather than an empty string. A password silently expanding to nothing produces an
// authentication failure a long way from its cause.

// envRefRe matches `${NAME}`. Bare `$NAME` is deliberately NOT matched: a `$` is a legal character
// in a password, and expanding one would corrupt a DSN that was already correct.
var envRefRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandDSNs replaces `${VAR}` references in every shard DSN.
//
// Applied to DSNs and nothing else. A general expansion over every string in the config would mean
// a table name, a listen address or a log format could change meaning depending on the environment,
// and none of those has a reason to.
func (c *Config) expandDSNs(env map[string]string) error {
	for i := range c.Shards {
		expanded, err := expandEnv(c.Shards[i].DSN, env)
		if err != nil {
			return fmt.Errorf("config: shards[%d] (%s): %w", i, c.Shards[i].ID, err)
		}
		c.Shards[i].DSN = expanded
	}
	return nil
}

// expandEnv replaces every `${VAR}` with its value.
//
// An unset variable is refused, naming it. The alternative — expanding to an empty string — turns a
// missing Secret key into an authentication failure against the database, which is reported by
// MySQL, blamed on the credentials, and has nothing in it pointing at the config. A variable that
// is SET to an empty value is honoured, because a database with no password is a real thing.
func expandEnv(s string, env map[string]string) (string, error) {
	if !strings.Contains(s, "${") {
		return s, nil
	}

	var missing []string
	out := envRefRe.ReplaceAllStringFunc(s, func(ref string) string {
		name := ref[2 : len(ref)-1]
		v, ok := env[name]
		if !ok {
			missing = append(missing, name)
			return ref
		}
		return v
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("%s is not set in the environment", strings.Join(missing, ", "))
	}

	// A `${` that survived is a reference this function does not understand — an unclosed brace, or
	// a name with a character the pattern excludes. Left alone it would be sent to MySQL as part of
	// a username, so it is refused instead.
	if strings.Contains(out, "${") {
		return "", fmt.Errorf("%q contains a ${…} reference that is not a plain variable name", s)
	}
	return out, nil
}
