package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// Invalidator is the subset of the cache the tailer needs.
//
// One method, declared by the consumer. The tailer cannot read entries and cannot fill them; it can
// only invalidate. That is not a convenience — a backstop that could write values would be a second
// source of truth, and reconciling two of those is the problem this design exists to avoid.
type Invalidator interface {
	Tombstone(ctx context.Context, key string, version uint64) (bool, error)
}

// Options configures a Tailer.
type Options struct {
	// ShardID names the shard being tailed. Each shard has its own binlog, its own HLC and its own
	// tailer; versions from different shards are incomparable (ADR 0003).
	ShardID string

	Addr     string
	User     string
	Password string
	Database string
	Table    string

	// ServerID is this tailer's replication server id. It must be unique across every replica and
	// tailer attached to the same MySQL instance, or the two fight over the connection.
	ServerID uint32

	Cache      Invalidator
	Checkpoint Checkpoint

	// CheckpointEvery bounds how much of the binlog is re-read after a restart. Saving on every
	// event would fsync per row; saving rarely means a longer replay. Replay is safe but not free,
	// so this is a throughput/recovery trade rather than a correctness one.
	CheckpointEvery time.Duration

	Logger *slog.Logger

	// OnInvalidate is called after each invalidation attempt. It exists so tests can observe
	// progress without polling the cache, and so the binary can count applied versus rejected.
	OnInvalidate func(key string, version uint64, applied bool)
}

// Tailer streams one shard's binlog and invalidates the rows it sees change.
type Tailer struct {
	opts  Options
	log   *slog.Logger
	canal *canal.Canal

	mu       sync.Mutex
	position Position
	dirty    bool
}

// New builds a Tailer and connects to the shard.
func New(opts Options) (*Tailer, error) {
	if opts.Cache == nil {
		return nil, errors.New("cdc: no invalidator")
	}
	if opts.Checkpoint == nil {
		return nil, errors.New("cdc: no checkpoint store")
	}
	if opts.ServerID == 0 {
		return nil, errors.New("cdc: server id must be set and unique per tailer")
	}
	if opts.Table == "" {
		return nil, errors.New("cdc: no table configured")
	}
	if opts.CheckpointEvery <= 0 {
		opts.CheckpointEvery = 5 * time.Second
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = opts.Addr
	cfg.User = opts.User
	cfg.Password = opts.Password
	cfg.ServerID = opts.ServerID
	cfg.Flavor = "mysql"
	// Only the cached table is streamed. Every other table's events are discarded before they reach
	// the invalidation path, which keeps an unrelated batch job from generating cache churn.
	cfg.IncludeTableRegex = []string{fmt.Sprintf("%s\\.%s", opts.Database, opts.Table)}
	cfg.Dump.ExecutionPath = "" // never mysqldump; Flux tails, it does not snapshot

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("cdc: connect to %s: %w", opts.Addr, err)
	}

	t := &Tailer{opts: opts, log: log, canal: c}
	c.SetEventHandler(&handler{tailer: t})
	return t, nil
}

// Run streams until ctx is cancelled, then saves its position.
//
// Starting position, in order of preference: the stored checkpoint, else the shard's CURRENT
// position. Starting from the beginning of the binlog on a first run would replay the entire
// history — safe, because invalidation is idempotent, but a long and pointless burst of cache
// churn on every fresh deployment.
func (t *Tailer) Run(ctx context.Context) error {
	start, found, err := t.opts.Checkpoint.Load()
	if err != nil {
		return err
	}
	if !found {
		pos, err := t.canal.GetMasterPos()
		if err != nil {
			return fmt.Errorf("cdc: read current binlog position: %w", err)
		}
		start = Position{File: pos.Name, Offset: pos.Pos}
		t.log.Info("no checkpoint; starting from the current position",
			"shard", t.opts.ShardID, "position", start)
	} else {
		t.log.Info("resuming from checkpoint", "shard", t.opts.ShardID, "position", start)
	}

	t.setPosition(start)

	// The periodic saver runs under the same lifetime as the stream, so there is no goroutine left
	// running after Run returns (CONTRIBUTING.md rule 2).
	var wg sync.WaitGroup
	saverDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.saveLoop(ctx, saverDone)
	}()

	runErr := make(chan error, 1)
	go func() {
		runErr <- t.canal.RunFrom(mysql.Position{Name: start.File, Pos: start.Offset})
	}()

	select {
	case <-ctx.Done():
		t.canal.Close()
		<-runErr
	case err := <-runErr:
		close(saverDone)
		wg.Wait()
		// Persist whatever progress was made before reporting the failure, so a crash does not also
		// cost the position.
		t.saveIfDirty()
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("cdc: tail %s: %w", t.opts.ShardID, err)
		}
		return nil
	}

	close(saverDone)
	wg.Wait()
	t.saveIfDirty()
	return ctx.Err()
}

// Close releases the replication connection.
func (t *Tailer) Close() { t.canal.Close() }

// Position returns the last position the tailer processed.
func (t *Tailer) Position() Position {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.position
}

func (t *Tailer) setPosition(p Position) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.position = p
	t.dirty = true
}

func (t *Tailer) saveLoop(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(t.opts.CheckpointEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.saveIfDirty()
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (t *Tailer) saveIfDirty() {
	t.mu.Lock()
	p, dirty := t.position, t.dirty
	t.dirty = false
	t.mu.Unlock()

	if !dirty || p.File == "" {
		return
	}
	if err := t.opts.Checkpoint.Save(p); err != nil {
		// A failed checkpoint save is not fatal: the tailer keeps working and simply replays more
		// after a restart, which is safe because invalidation is idempotent. It is logged because a
		// persistently failing save means recovery will be slow when it is eventually needed.
		t.log.Warn("checkpoint save failed", "shard", t.opts.ShardID, "position", p, "err", err)
	}
}

// invalidate tombstones one changed row.
func (t *Tailer) invalidate(ctx context.Context, id, version uint64) {
	key := t.opts.Table + ":" + strconv.FormatUint(id, 10)

	applied, err := t.opts.Cache.Tombstone(ctx, key, version)
	if err != nil {
		t.log.Warn("invalidation failed", "shard", t.opts.ShardID, "key", key, "err", err)
		return
	}
	if t.opts.OnInvalidate != nil {
		t.opts.OnInvalidate(key, version, applied)
	}
}

// handler receives canal's callbacks.
type handler struct {
	canal.DummyEventHandler
	tailer *Tailer
}

// OnRow invalidates every row touched by a binlog event.
//
// The version comes from the row's own `version` column, which is why the shard is configured with
// binlog_row_image=FULL: a minimal image omits unchanged columns, and an invalidation without a
// version cannot participate in the compare-and-set — it could only delete unconditionally, which
// reopens the delete-versus-fill race.
func (h *handler) OnRow(e *canal.RowsEvent) error {
	ctx := context.Background()

	idIdx, versionIdx := -1, -1
	for i, col := range e.Table.Columns {
		switch col.Name {
		case "id":
			idIdx = i
		case "version":
			versionIdx = i
		}
	}
	if idIdx < 0 || versionIdx < 0 {
		return fmt.Errorf("cdc: table %s.%s lacks an id or version column",
			e.Table.Schema, e.Table.Name)
	}

	// UPDATE events carry before/after pairs; only the AFTER image matters, since it holds the new
	// version. INSERT and DELETE carry one row each.
	step := 1
	offset := 0
	if e.Action == canal.UpdateAction {
		step = 2
		offset = 1
	}

	for i := offset; i < len(e.Rows); i += step {
		row := e.Rows[i]
		if idIdx >= len(row) || versionIdx >= len(row) {
			continue
		}
		id, ok := toUint64(row[idIdx])
		if !ok {
			continue
		}
		version, ok := toUint64(row[versionIdx])
		if !ok {
			continue
		}
		h.tailer.invalidate(ctx, id, version)
	}
	return nil
}

// OnPosSynced records progress. The position is only checkpointed periodically, because saving on
// every event would fsync per row.
func (h *handler) OnPosSynced(_ *replication.EventHeader, pos mysql.Position, _ mysql.GTIDSet, _ bool) error {
	h.tailer.setPosition(Position{File: pos.Name, Offset: pos.Pos})
	return nil
}

// String identifies the handler in canal's logs.
func (h *handler) String() string { return "cachet-flux" }

// toUint64 normalises the several numeric types canal produces for an unsigned column.
func toUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case int64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case int32:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	default:
		return 0, false
	}
}
