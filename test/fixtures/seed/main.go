package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"hash"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// defaultShardDSNs matches test/env/compose.yml. Overridable so the same command can seed a
// testcontainers stack, a CI stack, or someone's own database during evaluation (build plan §6.1
// step 2).
const defaultShardDSNs = "shard0=root:cachet@tcp(127.0.0.1:3316)/cachet," +
	"shard1=root:cachet@tcp(127.0.0.1:3317)/cachet," +
	"shard2=root:cachet@tcp(127.0.0.1:3318)/cachet"

func main() {
	if err := run(); err != nil {
		slog.Error("seed failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		profileName = flag.String("profile", "small", "fixture profile: small, medium, or large")
		dsns        = flag.String("shards", defaultShardDSNs, "comma-separated id=dsn pairs")
		batchSize   = flag.Int("batch", 1000, "rows per INSERT statement")
		truncate    = flag.Bool("truncate", true, "empty the tables before seeding")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	profile, err := ProfileByName(*profileName)
	if err != nil {
		return err
	}
	if *batchSize < 1 {
		return fmt.Errorf("seed: batch size must be positive, got %d", *batchSize)
	}

	// Ctrl-C must abort cleanly rather than leaving a half-seeded shard whose seed_meta claims a
	// complete load.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shards, err := openShards(ctx, *dsns)
	if err != nil {
		return err
	}
	defer closeShards(shards)

	ids := make([]storage.ShardID, 0, len(shards))
	for id := range shards {
		ids = append(ids, id)
	}
	router, err := storage.NewRouter(ids)
	if err != nil {
		return fmt.Errorf("seed: build router: %w", err)
	}

	if *truncate {
		if err := truncateAll(ctx, shards); err != nil {
			return err
		}
	}

	started := time.Now()
	loaders := make(map[storage.ShardID]*loader, len(shards))
	for id, db := range shards {
		loaders[id] = newLoader(id, db, *batchSize)
	}

	err = Generate(profile, func(rec storage.Record) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		id, err := router.ShardFor(CacheKey(rec.ID))
		if err != nil {
			return err
		}
		return loaders[id].add(ctx, rec)
	})
	if err != nil {
		return err
	}

	total := uint64(0)
	for id, l := range loaders {
		if err := l.flush(ctx); err != nil {
			return err
		}
		if err := l.recordMeta(ctx, profile); err != nil {
			return err
		}
		slog.Info("shard seeded", "shard", id, "rows", l.rows, "checksum", l.digest()[:16])
		total += l.rows
	}

	slog.Info("seed complete",
		"profile", profile.Name,
		"rows", total,
		"shards", len(shards),
		"took", time.Since(started).Round(time.Millisecond))
	return nil
}

func openShards(ctx context.Context, spec string) (map[storage.ShardID]*sql.DB, error) {
	out := make(map[storage.ShardID]*sql.DB)
	for _, pair := range strings.Split(spec, ",") {
		id, dsn, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok || id == "" || dsn == "" {
			closeShards(out)
			return nil, fmt.Errorf("seed: malformed shard spec %q, want id=dsn", pair)
		}
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			closeShards(out)
			return nil, fmt.Errorf("seed: open %s: %w", id, err)
		}
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			closeShards(out)
			return nil, fmt.Errorf("seed: ping %s: %w", id, err)
		}
		out[storage.ShardID(id)] = db
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("seed: no shards in %q", spec)
	}
	return out, nil
}

func closeShards(shards map[storage.ShardID]*sql.DB) {
	for _, db := range shards {
		_ = db.Close()
	}
}

func truncateAll(ctx context.Context, shards map[storage.ShardID]*sql.DB) error {
	for id, db := range shards {
		for _, table := range []string{"entities", "seed_meta"} {
			if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+table); err != nil {
				return fmt.Errorf("seed: truncate %s on %s: %w", table, id, err)
			}
		}
	}
	return nil
}

// loader batches rows for one shard.
//
// Rows are inserted in multi-row statements rather than one at a time: the large profile is ten
// million rows, and per-row round trips would make a full reseed take long enough that people stop
// resetting between runs — which is how benchmark results start depending on the previous run.
type loader struct {
	id    storage.ShardID
	db    *sql.DB
	clock *storage.Clock
	size  int

	pending []storage.Record
	rows    uint64
	hasher  hash.Hash
}

func newLoader(id storage.ShardID, db *sql.DB, size int) *loader {
	return &loader{
		id:      id,
		db:      db,
		clock:   storage.NewClock(time.Now),
		size:    size,
		pending: make([]storage.Record, 0, size),
		hasher:  sha256.New(),
	}
}

func (l *loader) add(ctx context.Context, rec storage.Record) error {
	HashRecord(l.hasher, rec)
	l.pending = append(l.pending, rec)
	if len(l.pending) >= l.size {
		return l.flush(ctx)
	}
	return nil
}

func (l *loader) flush(ctx context.Context) error {
	if len(l.pending) == 0 {
		return nil
	}

	// One version per batch, not per row. Versions order writes against each other; they do not
	// need to be unique, and burning ten million logical ticks to seed a fixture would push the
	// clock's physical component minutes into the future for no benefit.
	version := uint64(l.clock.Next())

	var sb strings.Builder
	sb.WriteString("INSERT INTO entities (id, tenant_id, status, payload, version) VALUES ")
	args := make([]any, 0, len(l.pending)*5)
	for i, r := range l.pending {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?)")
		args = append(args, r.ID, r.TenantID, r.Status, r.Payload, version)
	}

	if _, err := l.db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("seed: insert batch on %s: %w", l.id, err)
	}

	l.rows += uint64(len(l.pending))
	l.pending = l.pending[:0]
	return nil
}

func (l *loader) digest() string { return hex.EncodeToString(l.hasher.Sum(nil)) }

// recordMeta writes what was loaded, so the harness can answer "which profile is on this shard, and
// was it loaded completely?" without counting ten million rows.
func (l *loader) recordMeta(ctx context.Context, p Profile) error {
	shardNum := strings.TrimPrefix(string(l.id), "shard")
	_, err := l.db.ExecContext(ctx,
		`INSERT INTO seed_meta (shard_id, profile, row_count, seed, checksum)
		 VALUES (?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE row_count = VALUES(row_count), seed = VALUES(seed),
		                         checksum = VALUES(checksum), seeded_at = CURRENT_TIMESTAMP(3)`,
		shardNum, p.Name, l.rows, p.Seed, l.digest())
	if err != nil {
		return fmt.Errorf("seed: record meta on %s: %w", l.id, err)
	}
	return nil
}
