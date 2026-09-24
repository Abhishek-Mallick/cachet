package sextant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Reading a Redis somebody else fills.
//
// This adapter is the difference between a verifier for Cachet and a verifier for caches in
// general. It has to read entries it did not write, in a layout it was told about rather than one
// it chose, and it must be unable to change any of them.
//
// Read-only is enforced twice over, and neither is decoration. The wrapper exposes Peek and nothing
// else, so no caller can reach a writing method through it — and the documented deployment is a
// Redis ACL user with `+@read` only, so the connection cannot write even if this code were wrong.
// A verifier that could influence what it reports on is not a verifier.

// Codec is how a cached value carries the version a version-tier comparison needs.
type Codec string

const (
	// CodecRaw treats the whole value as opaque. Value-tier only: there is nothing to extract.
	CodecRaw Codec = "raw"

	// CodecJSON reads a version out of a JSON document by field name.
	CodecJSON Codec = "json"

	// CodecHash reads a version out of a Redis hash field.
	CodecHash Codec = "hash"
)

// Valid reports whether the codec is one this package knows.
func (c Codec) Valid() bool {
	switch c {
	case CodecRaw, CodecJSON, CodecHash:
		return true
	default:
		return false
	}
}

// ParseCodec reads a codec name from configuration.
func ParseCodec(s string) (Codec, error) {
	c := Codec(s)
	if !c.Valid() {
		return "", fmt.Errorf("sextant: unknown codec %q; want raw, json or hash", s)
	}
	return c, nil
}

// RedisCacheOptions configures a verifier's read-only view of a Redis.
type RedisCacheOptions struct {
	// Addrs is one address for a single server, or several for a cluster.
	Addrs    []string
	Username string
	Password string
	DB       int

	// KeyTemplate maps a verifier key onto the application's cache key. `{key}` is replaced; a
	// template without it is a constant, which would make every check read one entry.
	//
	// Declared because a cache key layout is the application's, not Cachet's: "user:{key}:v3" is as
	// likely as anything else, and a verifier that guessed would read nothing and report perfect
	// consistency.
	KeyTemplate string

	// Codec is how a value carries its version.
	Codec Codec

	// VersionField is the JSON field or hash field holding the version, for those codecs.
	VersionField string

	// ValueFields, for the hash codec, are the fields a value-tier comparison compares. Empty
	// compares the whole value for raw and JSON.
	ValueFields []string
}

// RedisCache reads entries from a Redis it must not be able to change.
type RedisCache struct {
	client redis.UniversalClient
	opts   RedisCacheOptions
}

// NewRedisCache connects read-only to a Redis and checks it answers.
func NewRedisCache(ctx context.Context, opts RedisCacheOptions) (*RedisCache, error) {
	if len(opts.Addrs) == 0 {
		return nil, errors.New("sextant: no cache addresses")
	}
	if !strings.Contains(opts.KeyTemplate, "{key}") {
		return nil, fmt.Errorf(
			"sextant: key_template %q does not contain {key}; every check would read the same entry",
			opts.KeyTemplate)
	}
	if opts.Codec == "" {
		opts.Codec = CodecRaw
	}
	if !opts.Codec.Valid() {
		return nil, fmt.Errorf("sextant: unknown codec %q", opts.Codec)
	}
	if opts.Codec != CodecRaw && opts.VersionField == "" {
		return nil, fmt.Errorf("sextant: codec %q needs a version field to extract", opts.Codec)
	}

	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    opts.Addrs,
		Username: opts.Username,
		Password: opts.Password,
		DB:       opts.DB,
		// A verifier samples; it does not serve traffic. Its load on the system it observes has to
		// be a deliberate number.
		PoolSize: 4,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("sextant: reach cache: %w", err)
	}
	return &RedisCache{client: client, opts: opts}, nil
}

// Close releases the connection.
func (c *RedisCache) Close() error { return c.client.Close() }

// CacheKey renders the application's key for a verifier key, for an operator checking the template
// against what their application actually writes.
func (c *RedisCache) CacheKey(key string) string {
	return strings.ReplaceAll(c.opts.KeyTemplate, "{key}", key)
}

// Peek returns what the cache holds for a key.
//
// The only method on this type that touches Redis, and it issues only GET or HGETALL. That is the
// read-only guarantee in the code; the ACL is the read-only guarantee in the deployment, and a
// verifier deserves both.
func (c *RedisCache) Peek(ctx context.Context, key string) (Entry, bool, error) {
	cacheKey := c.CacheKey(key)

	if c.opts.Codec == CodecHash {
		fields, err := c.client.HGetAll(ctx, cacheKey).Result()
		if err != nil {
			return Entry{}, false, fmt.Errorf("sextant: read %s: %w", cacheKey, err)
		}
		if len(fields) == 0 {
			return Entry{}, false, nil
		}
		return c.entryFromHash(cacheKey, fields)
	}

	raw, err := c.client.Get(ctx, cacheKey).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		// Nothing cached is nothing to be wrong about.
		return Entry{}, false, nil
	case err != nil:
		return Entry{}, false, fmt.Errorf("sextant: read %s: %w", cacheKey, err)
	}

	entry := Entry{Value: raw}
	if c.opts.Codec == CodecJSON {
		v, err := versionFromJSON(raw, c.opts.VersionField)
		if err != nil {
			return Entry{}, false, fmt.Errorf("sextant: %s: %w", cacheKey, err)
		}
		entry.Version, entry.VersionKnown = v, true
	}
	return entry, true, nil
}

func (c *RedisCache) entryFromHash(cacheKey string, fields map[string]string) (Entry, bool, error) {
	entry := Entry{}

	raw, ok := fields[c.opts.VersionField]
	if !ok {
		return Entry{}, false, fmt.Errorf(
			"sextant: %s has no field %q; the declared version field does not match what is cached",
			cacheKey, c.opts.VersionField)
	}
	v, err := parseVersion([]byte(raw))
	if err != nil {
		return Entry{}, false, fmt.Errorf("sextant: %s: field %q: %w", cacheKey, c.opts.VersionField, err)
	}
	entry.Version, entry.VersionKnown = v, true

	// The value projection is built in DECLARED field order. A map has no order, so building it
	// from iteration would produce a different projection on every read and every entry would look
	// stale.
	if len(c.opts.ValueFields) > 0 {
		values := make([]string, len(c.opts.ValueFields))
		for i, f := range c.opts.ValueFields {
			values[i] = fields[f]
		}
		entry.Value = []byte(strings.Join(values, "\x00"))
	}
	return entry, true, nil
}

// versionFromJSON extracts a version from a JSON document.
//
// A top-level field only. A path expression would be a small language to get wrong in a component
// whose value is being trustworthy, and an application that wants this verified can put the field
// where the verifier can see it.
func versionFromJSON(raw []byte, field string) (uint64, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return 0, fmt.Errorf("value is not a JSON object: %w", err)
	}
	v, ok := doc[field]
	if !ok {
		return 0, fmt.Errorf("no field %q; the declared version field does not match what is cached", field)
	}

	// A number or a string, because both are what a version ends up as once it has been through
	// somebody's serialiser — and a JSON number large enough to be an HLC version does not survive
	// a float64.
	s := strings.Trim(string(v), `"`)
	return parseVersion([]byte(s))
}
