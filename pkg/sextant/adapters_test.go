package sextant_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/Abhishek-Mallick/cachet/pkg/sextant"
)

// The adapters are where a verifier meets somebody else's deployment, so most of what matters here
// is what they REFUSE. A verifier that quietly read the wrong thing would report perfect
// consistency, which is the most misleading output this component can produce.

// TestAnOriginRefusesAnIdentifierThatCouldReachSQL. Identifiers arrive from configuration and
// become part of a statement; values never do. The pattern is the whole guarantee.
func TestAnOriginRefusesAnIdentifierThatCouldReachSQL(t *testing.T) {
	t.Parallel()

	base := sextant.SQLOriginOptions{
		DSNs: []string{"u:p@tcp(127.0.0.1:1)/db"}, Table: "orders",
		KeyColumn: "id", VersionColumn: "version",
	}

	for name, mutate := range map[string]func(*sextant.SQLOriginOptions){
		"a table with a backtick":   func(o *sextant.SQLOriginOptions) { o.Table = "orders`" },
		"a table with a space":      func(o *sextant.SQLOriginOptions) { o.Table = "orders; DROP TABLE x" },
		"a key column with a quote": func(o *sextant.SQLOriginOptions) { o.KeyColumn = `id"` },
		"an empty version column name": func(o *sextant.SQLOriginOptions) {
			o.VersionColumn = "1"
		},
		"a value column that is a subquery": func(o *sextant.SQLOriginOptions) {
			o.ValueColumns = []string{"(SELECT 1)"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			opts := base
			mutate(&opts)
			if _, err := sextant.NewSQLOrigin(context.Background(), opts); err == nil {
				t.Fatal("an identifier that could reach SQL was accepted")
			} else if !strings.Contains(err.Error(), "identifier") {
				t.Errorf("the refusal does not say what is wrong: %v", err)
			}
		})
	}
}

// TestAnOriginMustBeAbleToCompareSomething. An origin that reads neither a version nor a projection
// has nothing to compare, and a verifier built on it would report clean forever.
func TestAnOriginMustBeAbleToCompareSomething(t *testing.T) {
	t.Parallel()

	_, err := sextant.NewSQLOrigin(context.Background(), sextant.SQLOriginOptions{
		DSNs: []string{"u:p@tcp(127.0.0.1:1)/db"}, Table: "orders", KeyColumn: "id",
	})
	if err == nil {
		t.Fatal("an origin with nothing to compare was accepted")
	}
	if !strings.Contains(err.Error(), "version column") {
		t.Errorf("the refusal does not say what is missing: %v", err)
	}
}

// TestHashShardIsStable. A shard function that moved keys between runs would send the verifier to
// the wrong writer, where every key reads as missing — which looks like a healthy cache, since a
// missing row is nothing to be wrong about.
func TestHashShardIsStable(t *testing.T) {
	t.Parallel()

	shard := sextant.HashShard(4)
	seen := map[int]bool{}
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		first := shard(key)
		if first != shard(key) {
			t.Fatalf("HashShard is not deterministic for %q", key)
		}
		if first < 0 || first >= 4 {
			t.Fatalf("HashShard(%q) = %d, outside the four writers configured", key, first)
		}
		seen[first] = true
	}
	if len(seen) < 2 {
		t.Errorf("eight keys landed on %d writer(s); the hash is not spreading", len(seen))
	}
	if got := sextant.HashShard(1)("anything"); got != 0 {
		t.Errorf("a single writer got index %d, want 0", got)
	}
}

// TestACacheRefusesAKeyTemplateThatIsAConstant. Every check would read the same entry, and the run
// would report a consistency figure for one key while appearing to measure the whole keyspace.
func TestACacheRefusesAKeyTemplateThatIsAConstant(t *testing.T) {
	t.Parallel()

	_, err := sextant.NewRedisCache(context.Background(), sextant.RedisCacheOptions{
		Addrs: []string{"127.0.0.1:1"}, KeyTemplate: "user:fixed",
	})
	if err == nil {
		t.Fatal("a key template with no {key} was accepted")
	}
	if !strings.Contains(err.Error(), "{key}") {
		t.Errorf("the refusal does not say what is missing: %v", err)
	}
}

// TestACodecThatExtractsAVersionNeedsToKnowWhichField.
func TestACodecThatExtractsAVersionNeedsToKnowWhichField(t *testing.T) {
	t.Parallel()

	for _, codec := range []sextant.Codec{sextant.CodecJSON, sextant.CodecHash} {
		_, err := sextant.NewRedisCache(context.Background(), sextant.RedisCacheOptions{
			Addrs: []string{"127.0.0.1:1"}, KeyTemplate: "user:{key}", Codec: codec,
		})
		if err == nil {
			t.Fatalf("codec %q was accepted with no version field", codec)
		}
		if !strings.Contains(err.Error(), "version field") {
			t.Errorf("codec %q: the refusal does not say what is missing: %v", codec, err)
		}
	}
}

func TestParsingTiersAndCodecs(t *testing.T) {
	t.Parallel()

	for _, s := range []string{"value", "version", "cachet"} {
		if _, err := sextant.ParseTier(s); err != nil {
			t.Errorf("ParseTier(%q): %v", s, err)
		}
	}
	if _, err := sextant.ParseTier("bounded"); err == nil {
		t.Error("an unknown tier was accepted; a typo would silently change what is measured")
	}

	for _, s := range []string{"raw", "json", "hash"} {
		if _, err := sextant.ParseCodec(s); err != nil {
			t.Errorf("ParseCodec(%q): %v", s, err)
		}
	}
	if _, err := sextant.ParseCodec("protobuf"); err == nil {
		t.Error("an unknown codec was accepted")
	}
}

// ─── key sources ────────────────────────────────────────────────────────────────

// TestAFileKeySourceIsReproducible. A conformance run has to check the same keys in the same
// sequence, or two runs of it are not comparable.
func TestAFileKeySourceIsReproducible(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "keys.txt")
	body := "# keys from the incident\nuser:1\n\nuser:2\nuser:3\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	src, err := sextant.NewFileKeys(path)
	if err != nil {
		t.Fatalf("NewFileKeys: %v", err)
	}
	if src.Len() != 3 {
		t.Fatalf("read %d keys, want 3 — comments and blank lines are not keys", src.Len())
	}

	got := make([]string, 0, 6)
	for range 6 {
		k, ok := src.Next()
		if !ok {
			t.Fatal("a file source ran out of keys; it is meant to cycle")
		}
		got = append(got, k)
	}
	want := []string{"user:1", "user:2", "user:3", "user:1", "user:2", "user:3"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("key %d = %q, want %q", i, got[i], want[i])
		}
	}

	if src.Kind() != sextant.SourceFile || src.Degraded() {
		t.Errorf("a declared list reported kind=%q degraded=%t", src.Kind(), src.Degraded())
	}
}

func TestAnEmptyKeyFileIsRefused(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "keys.txt")
	if err := os.WriteFile(path, []byte("# nothing but comments\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := sextant.NewFileKeys(path); err == nil {
		t.Error("a key file with no keys was accepted; the verifier would sample nothing and report clean")
	}
}

// TestTheBinlogSourceIsNotDegradedAndScanIs is the statement the whole key-source design rests on.
func TestTheBinlogSourceIsNotDegradedAndScanIs(t *testing.T) {
	t.Parallel()

	binlog := sextant.NewRecentKeys(8)
	if binlog.Kind() != sextant.SourceBinlog || binlog.Degraded() {
		t.Errorf("the binlog source reported kind=%q degraded=%t", binlog.Kind(), binlog.Degraded())
	}

	scan, err := sextant.NewScanKeys(sextant.ScanKeysOptions{
		Client: stubRedis{}, Match: "user:*", Prefix: "user:",
	})
	if err != nil {
		t.Fatalf("NewScanKeys: %v", err)
	}
	if scan.Kind() != sextant.SourceScan {
		t.Errorf("the scan source reported kind=%q", scan.Kind())
	}
	if !scan.Degraded() {
		t.Error("SCAN did not report itself degraded. It enumerates CACHED keys, so a dropped " +
			"invalidation that left no entry cannot be sampled at all — the failure hides itself " +
			"by looking like an absent key")
	}
	if _, ok := scan.Next(); ok {
		t.Error("a scan source returned a key before anything had been scanned")
	}
	if scan.CompletedAPass() {
		t.Error("a scan source claimed a completed pass before scanning")
	}
}

func TestScanningNeedsAClientAndAPattern(t *testing.T) {
	t.Parallel()

	if _, err := sextant.NewScanKeys(sextant.ScanKeysOptions{Match: "user:*"}); err == nil {
		t.Error("a scan source with no client was accepted")
	}
	if _, err := sextant.NewScanKeys(sextant.ScanKeysOptions{Client: stubRedis{}}); err == nil {
		t.Error("a scan source with no pattern was accepted; it would enumerate the whole keyspace")
	}
}

// stubRedis satisfies the client interface without connecting to anything. The scan tests are
// about what the source REFUSES and what it reports about itself, neither of which needs a server.
type stubRedis struct{ redis.UniversalClient }
