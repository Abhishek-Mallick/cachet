// A three-act demonstration of why cache invalidation belongs inside the data layer.
//
// Each act caches the same rows in the same Valkey, against the same MySQL. Only the invalidation
// strategy changes:
//
//	Act 1  A TTL cache.                    Serves data known to be wrong, for as long as the TTL.
//	Act 2  Invalidate on every write.      Correct for point writes. Wrong for conditional ones.
//	Act 3  Cachet.                         Exact invalidation, resolved inside the transaction.
//
// Act 2 is the one worth watching. It is what most teams actually run, it is written by people who
// understand caching, and it is still wrong — because an application issues a statement and never
// learns its consequences.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"github.com/Abhishek-Mallick/cachet/pkg/cachet"
	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

const (
	dsn       = "root:cachet@tcp(127.0.0.1:3316)/cachet?parseTime=true&interpolateParams=true"
	cacheAddr = "127.0.0.1:6379"
	engine    = "tcp://127.0.0.1:7070"

	// Each act owns a disjoint id range and its own tenant, so acts cannot contaminate each other
	// and any act can be run alone on stage.
	actTTLBase    = 9_100_000
	actNaiveBase  = 9_200_000
	actCachetBase = 9_300_000

	tenantTTL    = 7001
	tenantNaive  = 7002
	tenantCachet = 7003

	statusPending = 0
	statusShipped = 1

	// Long on purpose. Act 1's whole argument is that this number is a wrongness budget, not a
	// correctness mechanism.
	demoTTL = time.Hour
)

// order is what the demo pretends the rows are. The engine calls them entities.
type order struct {
	id     uint64
	tenant uint32
	status uint8
	body   string
}

func main() {
	act := flag.String("act", "all", "which act to run: ttl, naive, cachet, or all")
	pause := flag.Duration("pause", 900*time.Millisecond, "pause between steps, so a viewer can read")
	flag.Parse()

	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("open mysql: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("mysql at 127.0.0.1:3316 is not reachable (%v)\n\nRun `make env-up` first.", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cacheAddr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("valkey at %s is not reachable (%v)\n\nRun `make env-up` first.", cacheAddr, err)
	}

	d := &demo{db: db, rdb: rdb, pause: *pause}

	var failures int
	switch *act {
	case "ttl":
		failures += d.actTTL(ctx)
	case "naive":
		failures += d.actNaive(ctx)
	case "cachet":
		failures += d.actCachet(ctx)
	case "all":
		failures += d.actTTL(ctx)
		failures += d.actNaive(ctx)
		failures += d.actCachet(ctx)
		d.verdict()
	default:
		log.Fatalf("unknown act %q", *act)
	}

	// Acts 1 and 2 are SUPPOSED to serve stale data; that is the demonstration. Only Act 3 failing
	// is a real failure, and it exits non-zero so this can run in CI as a test of the demo itself.
	if failures > 0 {
		os.Exit(1)
	}
}

type demo struct {
	db    *sql.DB
	rdb   *redis.Client
	pause time.Duration
}

// ---------------------------------------------------------------------------------------------
// Act 1 — a TTL cache
// ---------------------------------------------------------------------------------------------

func (d *demo) actTTL(ctx context.Context) int {
	d.banner(1, "A TTL cache", "The cache expires entries after an hour. Nothing tells it about writes.")

	orders := d.seed(ctx, actTTLBase, tenantTTL, 3)
	d.flushKeys(ctx, orders)

	o := orders[0]
	d.step("Read order %d through the cache (a miss — it goes to the database)", o.id)
	got := d.ttlRead(ctx, o.id)
	d.result("cache says: %q", got)

	d.step("Now the database changes. Someone ships the order.")
	d.mustExec(ctx, `UPDATE entities SET status = ?, payload = ? WHERE id = ?`,
		statusShipped, "SHIPPED — customer has been told", o.id)
	d.result("database now says: %q", d.dbRead(ctx, o.id))

	d.step("Read it through the cache again")
	got = d.ttlRead(ctx, o.id)
	d.result("cache says: %q", got)

	if strings.Contains(got, "SHIPPED") {
		d.bad("the cache somehow updated itself — check that Act 1 is not running against Cachet")
		return 1
	}
	d.lesson(
		"The cache is serving an order as PENDING that the database shipped.",
		fmt.Sprintf("It will keep doing that for up to %s. Shortening the TTL does not make this", demoTTL),
		"correct — it shortens the window in which it is wrong, and costs hit rate to do it.",
	)
	return 0
}

// ---------------------------------------------------------------------------------------------
// Act 2 — invalidate on every write
// ---------------------------------------------------------------------------------------------

func (d *demo) actNaive(ctx context.Context) int {
	d.banner(2, "Invalidate on every write",
		"The application deletes the cache key whenever it writes. This is what most teams run.")

	orders := d.seed(ctx, actNaiveBase, tenantNaive, 3)
	d.flushKeys(ctx, orders)

	one := orders[0]
	d.step("Warm all %d orders for customer %d", len(orders), tenantNaive)
	for _, o := range orders {
		d.ttlRead(ctx, o.id)
	}

	d.step("A point write: ship order %d, and delete its cache key", one.id)
	d.mustExec(ctx, `UPDATE entities SET status = ?, payload = ? WHERE id = ?`,
		statusShipped, "SHIPPED — point write", one.id)
	d.mustDel(ctx, one.id) // the application knows exactly which key it touched
	if got := d.ttlRead(ctx, one.id); !strings.Contains(got, "SHIPPED") {
		d.bad("even the point write went stale: %q", got)
		return 1
	}
	d.good("correct. The application knew the key, so it could invalidate it.")

	d.step("Now the write every real application eventually makes:")
	d.sql(`UPDATE entities SET status = 'shipped' WHERE customer_id = %d AND status = 'pending'`, tenantNaive)

	res := d.mustExec(ctx, `UPDATE entities SET status = ?, payload = ? WHERE tenant_id = ? AND status = ?`,
		statusShipped, "SHIPPED — conditional write", tenantNaive, statusPending)
	n, _ := res.RowsAffected()
	d.result("the database changed %d rows", n)

	d.step("Which cache keys should the application delete?")
	d.note("It does not know. It sent a predicate, not a list of rows. It can:")
	d.note("  · delete nothing            → the rows it did not name stay stale")
	d.note("  · flush the whole table     → correct, and the hit rate goes to zero")
	d.note("  · guess                     → wrong in a way nothing reports")
	d.note("There is no fourth option from inside the application.")

	stale := 0
	for _, o := range orders[1:] {
		got := d.ttlRead(ctx, o.id)
		db := d.dbRead(ctx, o.id)
		state := "stale"
		if strings.Contains(got, "SHIPPED") {
			state = "fresh"
		} else {
			stale++
		}
		d.result("order %d — cache: %-34q database: %q  [%s]", o.id, got, db, state)
	}

	if stale == 0 {
		d.bad("no row was left stale; the demo did not reproduce its own premise")
		return 1
	}
	d.lesson(
		fmt.Sprintf("%d of %d rows are stale, and the application has no way to know.", stale, len(orders)-1),
		"This is not a discipline problem. The application saw a statement, never its consequences.",
		"That information exists — inside the transaction that did the write.",
	)
	return 0
}

// ---------------------------------------------------------------------------------------------
// Act 3 — Cachet
// ---------------------------------------------------------------------------------------------

func (d *demo) actCachet(ctx context.Context) int {
	d.banner(3, "Cachet", "The cache sits in the read path, where the write path can reach it.")

	c, err := cachet.Dial(ctx, engine)
	if err != nil {
		d.bad("cannot reach the engine at %s (%v)", engine, err)
		d.note("Start it with:  ./bin/cachet -config examples/staleness-demo/cachet.yaml")
		return 1
	}
	defer func() { _ = c.Close() }()

	// Three pending orders that the write WILL match, and two already-shipped ones it will not.
	// The untouched pair is the point: Act 2's only correct option was to flush the whole table,
	// and this shows what exact invalidation buys instead.
	pending := d.seedVia(ctx, c, actCachetBase, tenantCachet, 3, statusPending)
	untouched := d.seedVia(ctx, c, actCachetBase+100, tenantCachet, 2, statusShipped)
	all := append(append([]uint64{}, pending...), untouched...)

	d.step("Warm all %d orders through Cachet (%d pending, %d already shipped)",
		len(all), len(pending), len(untouched))
	for _, id := range all {
		if _, err := c.Get(ctx, key(id)); err != nil {
			d.bad("read %d: %v", id, err)
			return 1
		}
	}
	d.step("Confirm they are genuinely cached — every read is a hit")
	// Asked at EVENTUAL deliberately. The question here is "does the cache still hold this entry",
	// which is a question about the CACHE, not about what a session is allowed to be served.
	//
	// It matters because a long-lived SESSION client reading several keys on one shard currently
	// reports a miss on all of them: a read advances the session watermark to that read's fill
	// version, so reading key B makes key A's entry look too old to serve. Probing at SESSION here
	// would make this act look like an invalidation failure when nothing is wrong with the
	// invalidation.
	for _, id := range all {
		got, err := c.Get(ctx, key(id), cachet.AtLevel(consistency.Eventual))
		if err != nil {
			d.bad("read %d: %v", id, err)
			return 1
		}
		if !got.Meta.CacheHit {
			d.bad("order %d did not come from the cache; the rest of this act would prove nothing", id)
			return 1
		}
	}
	d.good("all %d served from cache", len(all))

	d.step("The same conditional write, through Cachet this time")
	d.sql(`UPDATE entities SET status = 'shipped' WHERE customer_id = %d AND status = 'pending'`, tenantCachet)

	res, err := c.UpdateWhere(ctx, cachet.Predicate{
		TenantID: tenantCachet, MatchStatus: statusPending, SetStatus: statusShipped,
	})
	if err != nil {
		d.bad("conditional write: %v", err)
		return 1
	}
	d.result("matched %d rows, and resolved them to %d exact keys", res.Matched, len(res.AffectedKeys))
	d.note("Those keys were resolved INSIDE the transaction, with SELECT ... FOR UPDATE.")
	d.note("They were invalidated after the commit and before this call returned.")

	d.step("Read the matched orders back")
	stale := 0
	for _, id := range pending {
		got, err := c.Get(ctx, key(id))
		if err != nil {
			d.bad("read %d: %v", id, err)
			return 1
		}
		state := "fresh"
		if got.Record.Status != statusShipped {
			state = "STALE"
			stale++
		}
		d.result("order %d — status %d  [%s]", id, got.Record.Status, state)
	}
	if stale > 0 {
		d.bad("%d rows stale through Cachet — that is a bug, not a demonstration", stale)
		return 1
	}

	d.step("And the orders the write did NOT match — are they still cached?")
	evicted := 0
	for _, id := range untouched {
		got, err := c.Get(ctx, key(id), cachet.AtLevel(consistency.Eventual))
		if err != nil {
			d.bad("read %d: %v", id, err)
			return 1
		}
		state := "still cached"
		if !got.Meta.CacheHit {
			state = "EVICTED"
			evicted++
		}
		d.result("order %d — %s", id, state)
	}
	if evicted > 0 {
		d.bad("%d untouched rows were evicted: invalidation was wider than the write", evicted)
		return 1
	}

	d.lesson(
		"Every matched row is correct. Every unmatched row is still cached.",
		"Act 2's only correct option was to flush the table and lose the hit rate. This did not,",
		"because the write path knew exactly which rows it touched — it was inside the transaction",
		"that touched them. That is the whole difference, and it is a question of layering.",
	)
	return 0
}

func (d *demo) verdict() {
	fmt.Print("\n" + strings.Repeat("━", 78) + "\n")
	fmt.Println("  Act 1  TTL cache               served data the database had already changed")
	fmt.Println("  Act 2  invalidate-on-write     correct for point writes, stale for conditional ones")
	fmt.Println("  Act 3  Cachet                  correct, with the cache still warm")
	fmt.Println()
	fmt.Println("  Acts 1 and 2 are not strawmen. They are what is running in production today,")
	fmt.Println("  and neither of them can tell you when it is wrong.")
	fmt.Print(strings.Repeat("━", 78) + "\n\n")
}

// ---------------------------------------------------------------------------------------------
// The naive cache used by Acts 1 and 2 — deliberately small, so nothing is hidden
// ---------------------------------------------------------------------------------------------

func (d *demo) ttlRead(ctx context.Context, id uint64) string {
	k := key(id)
	if v, err := d.rdb.Get(ctx, k).Result(); err == nil {
		return v
	} else if !errors.Is(err, redis.Nil) {
		log.Fatalf("cache get: %v", err)
	}
	v := d.dbRead(ctx, id)
	if err := d.rdb.Set(ctx, k, v, demoTTL).Err(); err != nil {
		log.Fatalf("cache set: %v", err)
	}
	return v
}

func (d *demo) dbRead(ctx context.Context, id uint64) string {
	var status uint8
	var payload []byte
	err := d.db.QueryRowContext(ctx, `SELECT status, payload FROM entities WHERE id = ?`, id).
		Scan(&status, &payload)
	if err != nil {
		log.Fatalf("db read %d: %v", id, err)
	}
	return string(payload)
}

func (d *demo) mustDel(ctx context.Context, id uint64) {
	if err := d.rdb.Del(ctx, key(id)).Err(); err != nil {
		log.Fatalf("cache del: %v", err)
	}
}

func (d *demo) flushKeys(ctx context.Context, orders []order) {
	for _, o := range orders {
		d.mustDel(ctx, o.id)
	}
}

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

func (d *demo) seed(ctx context.Context, base uint64, tenant uint32, n int) []order {
	out := make([]order, 0, n)
	for i := 0; i < n; i++ {
		o := order{
			id: base + uint64(i), tenant: tenant, status: statusPending,
			body: fmt.Sprintf("PENDING — order %d", base+uint64(i)),
		}
		d.mustExec(ctx,
			`INSERT INTO entities (id, tenant_id, status, payload, version) VALUES (?, ?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE tenant_id=VALUES(tenant_id), status=VALUES(status),
			   payload=VALUES(payload), version=VALUES(version)`,
			o.id, o.tenant, o.status, o.body, 1)
		out = append(out, o)
	}
	return out
}

// seedVia writes through Cachet, so the engine maintains the version column as it must.
func (d *demo) seedVia(ctx context.Context, c *cachet.Client, base uint64, tenant uint32, n int, status uint8) []uint64 {
	ids := make([]uint64, 0, n)
	label := "PENDING"
	if status == statusShipped {
		label = "SHIPPED"
	}
	for i := 0; i < n; i++ {
		id := base + uint64(i)
		_, err := c.Put(ctx, key(id), cachet.Record{
			TenantID: tenant, Status: status,
			Payload: []byte(fmt.Sprintf("%s — order %d", label, id)),
		})
		if err != nil {
			log.Fatalf("seed %d: %v", id, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func (d *demo) mustExec(ctx context.Context, q string, args ...any) sql.Result {
	res, err := d.db.ExecContext(ctx, q, args...)
	if err != nil {
		log.Fatalf("exec: %v", err)
	}
	return res
}

func key(id uint64) string { return "entities:" + strconv.FormatUint(id, 10) }

// ---------------------------------------------------------------------------------------------
// Output, shaped for a screen recording
// ---------------------------------------------------------------------------------------------

const (
	dim    = "\033[2m"
	bold   = "\033[1m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	cyan   = "\033[36m"
	reset  = "\033[0m"
)

func (d *demo) banner(n int, title, subtitle string) {
	fmt.Printf("\n%s%s\n", bold, strings.Repeat("━", 78))
	fmt.Printf("  ACT %d · %s%s\n", n, title, reset)
	fmt.Printf("  %s%s%s\n", dim, subtitle, reset)
	fmt.Printf("%s%s%s\n", bold, strings.Repeat("━", 78), reset)
	d.wait()
}

func (d *demo) step(format string, a ...any) {
	fmt.Printf("\n%s▸%s %s\n", cyan, reset, fmt.Sprintf(format, a...))
	d.wait()
}

func (d *demo) sql(format string, a ...any) {
	fmt.Printf("  %s%s%s\n", yellow, fmt.Sprintf(format, a...), reset)
	d.wait()
}

func (d *demo) result(format string, a ...any) {
	fmt.Printf("    %s\n", fmt.Sprintf(format, a...))
	d.wait()
}

func (d *demo) note(format string, a ...any) {
	fmt.Printf("    %s%s%s\n", dim, fmt.Sprintf(format, a...), reset)
}

func (d *demo) good(format string, a ...any) {
	fmt.Printf("    %s✓%s %s\n", green, reset, fmt.Sprintf(format, a...))
	d.wait()
}

func (d *demo) bad(format string, a ...any) {
	fmt.Printf("    %s✗ %s%s\n", red, fmt.Sprintf(format, a...), reset)
}

func (d *demo) lesson(lines ...string) {
	fmt.Println()
	for _, l := range lines {
		fmt.Printf("  %s%s%s\n", bold, l, reset)
	}
	d.wait()
}

func (d *demo) wait() { time.Sleep(d.pause) }
