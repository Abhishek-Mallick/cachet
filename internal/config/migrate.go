package config

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
)

// Migrating a config written before Cachet had declared tables.
//
// Requiring `tables:` is the one breaking change in this release for a deployment that already had
// a working config, and a breaking change with no mechanical path is a breaking change users pay
// for twice: once to understand it and once to get it wrong. Every such deployment was caching the
// same table with the same shape, because that shape was compiled in and nothing else was possible.
// So the migration is not a guess — it is writing down what was already true.

// ErrAlreadyDeclared reports that the config has been migrated already.
var ErrAlreadyDeclared = errors.New("config: this file already declares topologies or tables")

// Migrate adds the topology and table declarations a pre-declaration config was missing.
//
// It APPENDS to the original text rather than re-serialising the document. A config file is
// something a person wrote, with comments explaining why a timeout is what it is; round-tripping it
// through a YAML marshaller would silently delete all of that, and an operator would find out by
// missing it later.
func Migrate(src []byte) ([]byte, error) {
	k := koanf.New(".")
	if err := k.Load(rawbytes.Provider(src), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	if k.Exists("tables") || k.Exists("topologies") {
		return nil, ErrAlreadyDeclared
	}

	var doc struct {
		Shards []Shard `koanf:"shards"`
	}
	if err := k.Unmarshal("", &doc); err != nil {
		return nil, fmt.Errorf("config: read shards: %w", err)
	}
	if len(doc.Shards) == 0 {
		return nil, errors.New("config: this file declares no shards, so there is nothing to build a topology from")
	}

	ids := make([]string, 0, len(doc.Shards))
	for i, sh := range doc.Shards {
		if sh.ID == "" {
			return nil, fmt.Errorf("config: shards[%d] has no id", i)
		}
		ids = append(ids, sh.ID)
	}

	var out bytes.Buffer
	out.Write(src)
	if len(src) > 0 && !bytes.HasSuffix(src, []byte("\n")) {
		out.WriteByte('\n')
	}
	out.WriteString(migrationBlock(ids))
	return out.Bytes(), nil
}

// migrationBlock renders the declaration Cachet used to compile in.
//
// One topology holding every shard, because that is what the old build did: there was one table and
// it lived on every shard in the list. A deployment that wants a table sharded differently declares
// a second topology and moves the table to it — which it could not express at all before.
func migrationBlock(shardIDs []string) string {
	var b strings.Builder
	b.WriteString(`
# ─── Added by `)
	b.WriteString("`cachetctl config migrate`")
	b.WriteString(` ────────────────────────────────────────────
#
# Cachet no longer has a built-in table. This is the declaration it used to compile in, written
# down: the same five columns in the same order, so the row fingerprint is unchanged and every
# cache entry you already have still reads as a hit.
#
# A topology is a named set of shards. Tables name one rather than listing shards, because routing
# comes from the key and the key is namespaced by table — two tables are only safely read together
# if they are sharded the same way.
topologies:
  - name: main
    shards: [`)
	b.WriteString(strings.Join(shardIDs, ", "))
	b.WriteString(`]

tables:
  - name: entities
    topology: main
    primary_key: [id]

    # Maintained by Cachet on every write, never by your application. It is what every cache
    # compare-and-set is performed against.
    version_column: version

    # Order is not cosmetic: a column's position is part of the row encoding and of the
    # fingerprint. Reordering these invalidates every entry rather than decoding old bytes into
    # new columns.
    columns:
      - {name: id, type: uint64}
      - {name: tenant_id, type: uint32}
      - {name: status, type: uint8}
      - {name: payload, type: bytes}
      - {name: version, type: uint64}

    # The conditional-write shapes this deployment permits. Declared rather than taken from a
    # request, and each match set is checked against the table's indexes at boot — an unindexed
    # match takes gap locks inside the transaction that is about to write.
    predicates:
      - match: [tenant_id, status]
        set: [status]
`)
	return b.String()
}
