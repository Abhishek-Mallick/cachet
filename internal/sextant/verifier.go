package sextant

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

// CacheReader is the part of the cache Sextant needs.
//
// Read-only, and declared by the consumer for a reason that is not stylistic: a verifier that could
// write to the cache it is checking would be able to influence the thing it reports on. Whatever
// Sextant says about consistency has to be an observation, never a side effect.
type CacheReader interface {
	// Peek returns the fill version of a cached entry, and whether one is present.
	Peek(ctx context.Context, key string) (fillVersion uint64, present bool, err error)
}

// OriginReader is the part of the database Sextant needs.
type OriginReader interface {
	// Version returns the row's current version, and whether the row exists.
	Version(ctx context.Context, key string) (version uint64, exists bool, err error)

	// Shard names the shard a key belongs to, for attribution.
	Shard(key string) (string, error)
}

// KeySource supplies the keys to check.
//
// Weighted toward recently invalidated keys (build plan §10.5): a key nobody has written is a key
// that cannot be stale, so sampling uniformly would spend most of the budget confirming that
// untouched rows are still correct.
type KeySource interface {
	Next() (key string, ok bool)
}

// VerifierOptions configures a Verifier.
type VerifierOptions struct {
	Cache  CacheReader
	Origin OriginReader
	Keys   KeySource

	Bound  PropagationBound
	SLO    *SLO
	Tracer *Tracer

	// Interval is how long between sampling rounds, and Batch is how many keys each round checks.
	// Together they are the load Sextant puts on the system it is observing, which must be a
	// deliberate number rather than "as fast as possible".
	Interval time.Duration
	Batch    int

	// Shadow marks this verifier as observing a deployment that serves no application traffic. It
	// changes nothing about the arithmetic and everything about how the result should be read, so
	// it is carried through to the report rather than left to the operator to remember.
	Shadow bool

	Now    func() time.Time
	Logger *slog.Logger

	// OnViolation is called for each violation found, so a caller can log, alert, or collect them.
	OnViolation func(Violation)
}

// Verifier is the detection loop.
//
// It samples keys, compares the cache against the database, and decides whether the difference is a
// benign in-flight race or a real violation — the distinction that makes this a verifier rather
// than a monitor, and the genuinely hard part of the whole component.
type Verifier struct {
	opts VerifierOptions
	log  *slog.Logger
	now  func() time.Time

	// behindSince remembers when each key was FIRST seen behind, which is what makes the
	// propagation bound a duration rather than a guess. A single sample cannot distinguish a
	// momentary race from a permanent one; two samples of the same key can.
	mu          sync.Mutex
	behindSince map[string]time.Time

	stats Stats
}

// Stats is what a verifier has done.
type Stats struct {
	Rounds     int
	Checked    int
	Violations int
	Errors     int
}

// NewVerifier builds a Verifier.
func NewVerifier(opts VerifierOptions) (*Verifier, error) {
	switch {
	case opts.Cache == nil:
		return nil, errors.New("sextant: no cache reader")
	case opts.Origin == nil:
		return nil, errors.New("sextant: no origin reader")
	case opts.Keys == nil:
		return nil, errors.New("sextant: no key source")
	case opts.SLO == nil:
		return nil, errors.New("sextant: no SLO")
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Second
	}
	if opts.Batch <= 0 {
		opts.Batch = 100
	}
	if opts.Tracer == nil {
		opts.Tracer = NewTracer(TracerOptions{})
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Verifier{
		opts:        opts,
		log:         opts.Logger,
		now:         opts.Now,
		behindSince: make(map[string]time.Time),
	}, nil
}

// Run samples until the context is cancelled.
func (v *Verifier) Run(ctx context.Context) error {
	ticker := time.NewTicker(v.opts.Interval)
	defer ticker.Stop()

	v.log.InfoContext(ctx, "sextant started",
		"shadow", v.opts.Shadow,
		"propagation_bound", v.opts.Bound.Duration(),
		"interval", v.opts.Interval,
		"batch", v.opts.Batch)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			v.RunOnce(ctx)
		}
	}
}

// RunOnce performs one sampling round.
//
// Exported so a test can drive the loop deterministically rather than sleeping through intervals
// and hoping. A verifier whose behaviour can only be observed in real time is one whose detection
// logic never gets tested properly.
func (v *Verifier) RunOnce(ctx context.Context) Stats {
	round := Stats{}

	for i := 0; i < v.opts.Batch; i++ {
		key, ok := v.opts.Keys.Next()
		if !ok {
			break
		}
		if err := v.check(ctx, key, &round); err != nil {
			round.Errors++
			v.log.WarnContext(ctx, "sextant check failed", "key", key, "err", err)
		}
	}

	v.mu.Lock()
	v.stats.Rounds++
	v.stats.Checked += round.Checked
	v.stats.Violations += round.Violations
	v.stats.Errors += round.Errors
	v.mu.Unlock()

	round.Rounds = 1
	return round
}

func (v *Verifier) check(ctx context.Context, key string, round *Stats) error {
	dbVersion, exists, err := v.opts.Origin.Version(ctx, key)
	if err != nil {
		return fmt.Errorf("origin: %w", err)
	}
	fillVersion, present, err := v.opts.Cache.Peek(ctx, key)
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}

	// Nothing cached is nothing to be wrong about. A missing entry is a miss, which costs a database
	// read and no correctness.
	if !present || !exists {
		v.forget(key)
		return nil
	}

	round.Checked++
	now := v.now()

	if fillVersion >= dbVersion {
		// Caught up — including the case where the entry is ahead, which happens when the cache was
		// read after a write landed and the database before it.
		v.forget(key)
		v.observeClean(now)
		return nil
	}

	shard, err := v.opts.Origin.Shard(key)
	if err != nil {
		return fmt.Errorf("shard: %w", err)
	}

	obs := Observation{
		Key:         key,
		Shard:       shard,
		DBVersion:   dbVersion,
		FillVersion: fillVersion,
		BehindSince: v.firstSeenBehind(key, now),
		Now:         now,
		Trace:       v.opts.Tracer.Trace(key),
	}

	violation, isViolation := Classify(obs, v.opts.Bound)
	if !isViolation {
		// Behind, but within the propagation bound — an invalidation is in flight and the model
		// never promised otherwise. Counting this would make every healthy system report violations
		// continuously, and the number would stop meaning anything within a day.
		v.observeClean(now)
		return nil
	}

	round.Violations++
	for _, level := range []consistency.Level{consistency.Session, consistency.Bounded, consistency.Eventual} {
		v.opts.SLO.Observe(level, now, violation.Violates(level))
	}
	if v.opts.OnViolation != nil {
		v.opts.OnViolation(violation)
	}
	return nil
}

// observeClean records one clean observation against every level that reads the cache.
func (v *Verifier) observeClean(now time.Time) {
	for _, level := range []consistency.Level{consistency.Session, consistency.Bounded, consistency.Eventual} {
		v.opts.SLO.Observe(level, now, false)
	}
}

// firstSeenBehind returns when this key was first observed behind, recording now if this is the
// first time.
func (v *Verifier) firstSeenBehind(key string, now time.Time) time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()

	if since, ok := v.behindSince[key]; ok {
		return since
	}
	v.behindSince[key] = now
	return now
}

// forget clears a key's behind-since marker once it has caught up, so a key that goes stale again
// later is timed from the new occasion rather than the old one.
func (v *Verifier) forget(key string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.behindSince, key)
}

// Stats returns what this verifier has observed.
func (v *Verifier) Stats() Stats {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.stats
}

// Shadow reports whether this verifier is observing a deployment serving no application traffic.
func (v *Verifier) Shadow() bool { return v.opts.Shadow }
