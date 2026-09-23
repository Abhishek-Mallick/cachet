package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/Abhishek-Mallick/cachet/internal/schema"
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

	// Descriptor declares the table's shape. When set, the primary key columns and the version
	// column are resolved by name from it and keys are built with the shared grammar — so a table
	// whose key is not called `id`, or is composite, produces the key the engine would.
	//
	// Nil keeps the historical behaviour: columns literally named `id` and `version`. That is what
	// the fixture uses, and the two agree byte-for-byte on it.
	Descriptor *schema.Descriptor

	// ServerID is this tailer's replication server id. It must be unique across every replica and
	// tailer attached to the same MySQL instance, or the two fight over the connection.
	ServerID uint32

	Cache      Invalidator
	Checkpoint Checkpoint

	// CheckpointEvery bounds how much of the binlog is re-read after a restart. Saving on every
	// event would fsync per row; saving rarely means a longer replay. Replay is safe but not free,
	// so this is a throughput/recovery trade rather than a correctness one.
	CheckpointEvery time.Duration

	// InvalidateRetryFor bounds retrying one invalidation against an unreachable cache. Zero takes
	// defaultInvalidateRetryFor. See invalidate for why this is not simply best-effort.
	InvalidateRetryFor time.Duration

	Logger *slog.Logger

	// OnInvalidate is called after each invalidation attempt. It exists so tests can observe
	// progress without polling the cache, and so the binary can count applied versus rejected.
	OnInvalidate func(key string, version uint64, applied bool)
}

const (
	defaultCheckpointEvery = 5 * time.Second

	// defaultInvalidateRetryFor bounds how long the tailer will keep trying to deliver one
	// invalidation. Long enough to ride out a cache restart or a failover; short enough that a
	// genuinely dead cache freezes the checkpoint and says so rather than stalling forever.
	defaultInvalidateRetryFor = 30 * time.Second

	initialInvalidateBackoff = 100 * time.Millisecond
	maxInvalidateBackoff     = 2 * time.Second
)

// Tailer streams one shard's binlog and invalidates the rows it sees change.
type Tailer struct {
	opts  Options
	log   *slog.Logger
	canal *canal.Canal

	mu       sync.Mutex
	position Position
	dirty    bool
	// frozen stops the checkpoint advancing past an invalidation that could not be delivered.
	// Everything after that point must be replayed on restart, which is safe because invalidation
	// is a versioned compare-and-set and therefore idempotent.
	frozen   bool
	frozenAt Position
}

// Frozen reports whether the tailer has stopped advancing its checkpoint, and where.
//
// A frozen checkpoint is the visible symptom of an invalidation that never landed. It is what turns
// "some rows are stale and nobody knows why" into a question an operator can answer: `cachetctl
// checkpoints` shows a position that has stopped moving while the binlog has not.
func (t *Tailer) Frozen() (Position, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.frozenAt, t.frozen
}

// freeze pins the checkpoint at the last position known to be fully applied.
func (t *Tailer) freeze(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.frozen {
		return
	}
	t.frozen = true
	t.frozenAt = t.position
	t.log.Error("invalidation could not be delivered; freezing the checkpoint so a restart replays it",
		"shard", t.opts.ShardID, "key", key, "position", t.frozenAt)
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
		opts.CheckpointEvery = defaultCheckpointEvery
	}
	if opts.InvalidateRetryFor == 0 {
		opts.InvalidateRetryFor = defaultInvalidateRetryFor
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
	if t.frozen {
		// Record the last position everything before which is known to have been applied. Replaying
		// from here after a restart re-delivers the invalidation that was lost.
		p = t.frozenAt
	}
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

// invalidate tombstones one changed row, retrying while the cache is unreachable.
//
// Dropping the event on the first error was the original behaviour and it was wrong in a specific,
// quiet way. During a cache partition the write path's invalidation fails too, so BOTH paths lose
// the event; the entry then survives until its TTL with nothing reporting it. A backstop that a
// cache blip defeats is not a backstop.
//
// Retrying blocks this shard's stream for up to InvalidateRetryFor. That is deliberate: the tailer
// cannot honestly record progress it has not made, and stalling one shard's binlog consumption is a
// smaller harm than serving a stale row indefinitely. If the budget runs out, the checkpoint is
// frozen so a restart replays from before the lost event.
func (t *Tailer) invalidate(ctx context.Context, key string, version uint64) {
	backoff := initialInvalidateBackoff
	deadline := time.Now().Add(t.opts.InvalidateRetryFor)
	for attempt := 1; ; attempt++ {
		applied, err := t.opts.Cache.Tombstone(ctx, key, version)
		if err == nil {
			if attempt > 1 {
				t.log.Info("invalidation delivered after retrying",
					"shard", t.opts.ShardID, "key", key, "attempts", attempt)
			}
			if t.opts.OnInvalidate != nil {
				t.opts.OnInvalidate(key, version, applied)
			}
			return
		}

		if ctx.Err() != nil || !time.Now().Before(deadline) {
			t.log.Warn("invalidation failed", "shard", t.opts.ShardID, "key", key,
				"attempts", attempt, "err", err)
			t.freeze(key)
			return
		}

		select {
		case <-ctx.Done():
			t.freeze(key)
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxInvalidateBackoff {
			backoff = maxInvalidateBackoff
		}
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

	// Which columns name the row, and which carries the version. From the descriptor when one is
	// declared, so a table whose key is not called `id` works; falling back to the historical
	// names, which is what the fixture uses and what keeps its keys byte-identical.
	keyNames, versionName := []string{"id"}, "version"
	if d := h.tailer.opts.Descriptor; d != nil {
		keyNames = keyNames[:0]
		for _, c := range d.PrimaryKey {
			keyNames = append(keyNames, c.Name)
		}
		versionName = d.VersionColumn.Name
	}

	keyIdx := make([]int, len(keyNames))
	for i := range keyIdx {
		keyIdx[i] = -1
	}
	versionIdx := -1
	for i, col := range e.Table.Columns {
		for j, want := range keyNames {
			if col.Name == want {
				keyIdx[j] = i
			}
		}
		if col.Name == versionName {
			versionIdx = i
		}
	}
	for j, idx := range keyIdx {
		if idx < 0 {
			return fmt.Errorf("cdc: table %s.%s lacks the primary key column %q",
				e.Table.Schema, e.Table.Name, keyNames[j])
		}
	}
	if versionIdx < 0 {
		return fmt.Errorf("cdc: table %s.%s lacks the version column %q",
			e.Table.Schema, e.Table.Name, versionName)
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
		if versionIdx >= len(row) {
			continue
		}
		version, ok := toUint64(row[versionIdx])
		if !ok {
			continue
		}

		// The table name comes from the EVENT, not from configuration: one tailer may follow
		// several tables on a shard, and the key has to name the one that actually changed.
		key, ok := rowKey(e.Table.Name, keyIdx, row)
		if !ok {
			continue
		}
		h.tailer.invalidate(ctx, key, version)
	}
	return nil
}

// rowKey builds the cache key naming a changed row.
//
// It uses the same grammar the engine uses, so a tailer and an engine agree on what a row is called
// — which is the whole requirement: an invalidation under a key nobody reads is an invalidation
// that silently does nothing.
func rowKey(table string, keyIdx []int, row []any) (string, bool) {
	values := make([]any, len(keyIdx))
	for i, idx := range keyIdx {
		if idx >= len(row) || row[idx] == nil {
			return "", false
		}
		values[i] = binlogValue(row[idx])
	}
	k, err := schema.KeyOf(table, values...)
	if err != nil {
		return "", false
	}
	return k.String(), true
}

// binlogValue renders one binlog column value as the bytes the key grammar expects.
//
// canal produces Go types that vary by column type, and a key built from a fmt of the wrong one
// would name a row nobody reads. Integers are rendered as plain decimal, matching what the engine
// derives from the same column.
func binlogValue(v any) any {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	default:
		if u, ok := toUint64(v); ok {
			return u
		}
		return fmt.Sprint(v)
	}
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
