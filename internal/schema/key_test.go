package schema_test

import (
	"strings"
	"testing"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
)

// The single most important assertion in the generic-table work.
//
// A cache key is what the hash ring hashes. If the grammar changes the bytes of an existing key,
// every entry in every deployment moves to a different node on upgrade — a full-cache miss and an
// origin stampede, arriving silently as "the cache got slower".
func TestTheFixtureKeyIsByteIdentical(t *testing.T) {
	t.Parallel()

	d, err := schema.NewDescriptor(entities())
	if err != nil {
		t.Fatal(err)
	}
	k, err := d.Key(uint64(123))
	if err != nil {
		t.Fatal(err)
	}
	if got := k.String(); got != "entities:123" {
		t.Fatalf("key = %q, want %q — the ring would remap on upgrade", got, "entities:123")
	}
}

func TestAKeyRoundTrips(t *testing.T) {
	t.Parallel()

	for _, s := range []string{
		"entities:1", "entities:18446744073709551615",
		"users:alice", "users:a%3Ab", "orders:7:alice",
	} {
		k, err := schema.ParseKey(s)
		if err != nil {
			t.Errorf("ParseKey(%q): %v", s, err)
			continue
		}
		if got := k.String(); got != s {
			t.Errorf("round trip: %q → %q", s, got)
		}
	}
}

// Injectivity is a correctness property, not tidiness: two primary keys that produce one cache key
// means one row's value served for another row.
func TestDistinctValuesNeverProduceOneKey(t *testing.T) {
	t.Parallel()

	d, err := schema.NewDescriptor(schema.TableConfig{
		Name: "t", PrimaryKey: []string{"a", "b"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "a", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "b", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "version", Type: schema.Uint64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Each pair is a classic separator-injection collision: ("a:b","c") and ("a","b:c") would
	// collide on a naive join, as would ("a%3Ab","c") against an escaped ("a:b","c").
	pairs := [][2]string{
		{"a:b", "c"},
		{"a", "b:c"},
		{"a%3Ab", "c"},
		{"a:b", "c"},
		{"", "x"},
		{"x", ""},
		{"%", ":"},
		{":", "%"},
		{"a%", "b"},
		{"a", "%b"},
	}
	seen := map[string][2]string{}
	for _, p := range pairs {
		k, err := d.Key(p[0], p[1])
		if err != nil {
			t.Fatalf("Key(%q,%q): %v", p[0], p[1], err)
		}
		s := k.String()
		if prev, dup := seen[s]; dup && prev != p {
			t.Errorf("%v and %v both produce %q", prev, p, s)
		}
		seen[s] = p

		// And it must come back as what went in.
		back, err := schema.ParseKey(s)
		if err != nil {
			t.Fatalf("ParseKey(%q): %v", s, err)
		}
		if len(back.Values) != 2 || back.Values[0] != p[0] || back.Values[1] != p[1] {
			t.Errorf("%q decoded to %q, want %q", s, back.Values, p[:])
		}
	}
}

func TestAKeyMustMatchThePrimaryKeyArity(t *testing.T) {
	t.Parallel()

	d, _ := schema.NewDescriptor(entities())
	if _, err := d.Key(); err == nil {
		t.Error("accepted a key with no values")
	}
	if _, err := d.Key(uint64(1), uint64(2)); err == nil {
		t.Error("accepted two values for a single-column primary key")
	}
}

func TestMalformedKeysAreRefused(t *testing.T) {
	t.Parallel()

	for _, s := range []string{
		"", "entities", ":123", "1entities:2",
		"entities:%", "entities:%zz", "entities:%4", "ent ities:1",
	} {
		if _, err := schema.ParseKey(s); err == nil {
			t.Errorf("ParseKey(%q) was accepted", s)
		}
	}
}

// A key is read by cachetctl and printed in logs. Control bytes there are an operational hazard,
// not a correctness one, but escaping them costs nothing.
// An empty string is a legal PRIMARY KEY value for a VARCHAR column. Refusing it here would make
// one legitimate row uncacheable and — worse — unparseable by the CDC tailer, which recovers a
// primary key from the key string. Parsing is descriptor-free and cannot know the column type, so
// rejecting an empty integer key is the descriptor path's job, not the grammar's.
func TestAnEmptyStringPrimaryKeyValueIsLegal(t *testing.T) {
	t.Parallel()

	k, err := schema.ParseKey("users:")
	if err != nil {
		t.Fatalf("ParseKey(\"users:\"): %v", err)
	}
	if len(k.Values) != 1 || k.Values[0] != "" {
		t.Fatalf("values = %q, want one empty value", k.Values)
	}
	if got := k.String(); got != "users:" {
		t.Errorf("round trip gave %q", got)
	}
}

func TestControlBytesAreEscaped(t *testing.T) {
	t.Parallel()

	d, _ := schema.NewDescriptor(schema.TableConfig{
		Name: "t", PrimaryKey: []string{"a"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "a", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "version", Type: schema.Uint64},
		},
	})
	k, err := d.Key("a\nb")
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(k.String(), "\n\r\x00") {
		t.Errorf("key contains a raw control byte: %q", k.String())
	}
	back, _ := schema.ParseKey(k.String())
	if back.Values[0] != "a\nb" {
		t.Errorf("escaping was not reversible: %q", back.Values[0])
	}
}

// The tailer builds keys without a descriptor, from column values it reads out of a binlog event.
// It must produce exactly what the engine produces from the same row, or an invalidation lands
// under a key nobody reads — which is silent staleness, and the failure mode with no signal.
func TestTheTailerAndTheEngineAgreeOnAKey(t *testing.T) {
	t.Parallel()

	d, err := schema.NewDescriptor(entities())
	if err != nil {
		t.Fatal(err)
	}

	for _, id := range []uint64{1, 123, 18446744073709551615} {
		fromEngine, err := d.Key(id)
		if err != nil {
			t.Fatal(err)
		}
		fromTailer, err := schema.KeyOf("entities", id)
		if err != nil {
			t.Fatal(err)
		}
		if fromEngine.String() != fromTailer.String() {
			t.Errorf("id %d: engine says %q, tailer says %q", id, fromEngine, fromTailer)
		}
	}

	// And a string key, where the escaping has to match too.
	users, err := schema.NewDescriptor(schema.TableConfig{
		Name: "users", PrimaryKey: []string{"email"}, VersionColumn: "version",
		Columns: []schema.ColumnConfig{
			{Name: "email", Type: schema.String, Collation: "utf8mb4_bin"},
			{Name: "version", Type: schema.Uint64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, email := range []string{"a@b.com", "weird:value", "100%"} {
		fromEngine, _ := users.Key(email)
		fromTailer, err := schema.KeyOf("users", email)
		if err != nil {
			t.Fatal(err)
		}
		if fromEngine.String() != fromTailer.String() {
			t.Errorf("%q: engine says %q, tailer says %q", email, fromEngine, fromTailer)
		}
	}
}
