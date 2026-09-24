package sextant

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Where the keys to check come from, and why it matters which.
//
// A verifier samples. What it samples decides what it can find, so the source is not an
// implementation detail — it is the difference between measuring the interesting case and measuring
// the quiet one.
//
// The binlog is primary. It enumerates rows that CHANGED, which is exactly the population a stale
// entry can be hiding in, and it comes from the database rather than from the cache — so the cache
// cannot steer what it is checked on. That last property is why a verifier fed by the cache is a
// weaker instrument than one fed by the origin, and it is why the alternatives below are labelled.

// KeySourceKind names where a key source draws from, for the label on the metrics.
type KeySourceKind string

const (
	// SourceBinlog enumerates changed rows from replication. The population a stale entry lives in.
	SourceBinlog KeySourceKind = "binlog"

	// SourceFile reads a fixed list. Exact and reproducible, which is what a conformance run wants.
	SourceFile KeySourceKind = "file"

	// SourceScan enumerates CACHED keys. Degraded, and labelled so.
	SourceScan KeySourceKind = "scan"
)

// DescribedKeySource is a key source that says what it draws from.
//
// The kind travels with the source because a run sampled by SCAN and a run sampled by binlog are
// not measuring the same thing, and a number that does not say which is not comparable with
// anybody else's.
type DescribedKeySource interface {
	KeySource
	Kind() KeySourceKind

	// Degraded reports that this source systematically under-samples the interesting case.
	Degraded() bool
}

// ─── a fixed list ───────────────────────────────────────────────────────────────

// FileKeys is a key source reading a newline-delimited file.
//
// Exact and reproducible: a conformance run needs to check the same keys every time, and a
// production incident often has a list of the keys somebody is worried about.
type FileKeys struct {
	mu   sync.Mutex
	keys []string
	next int
}

// NewFileKeys reads a newline-delimited key file.
//
// Blank lines and lines beginning with # are skipped, so an operator can annotate the list they
// pasted out of an incident channel.
func NewFileKeys(path string) (*FileKeys, error) {
	f, err := os.Open(path) //nolint:gosec // the path is the operator's own argument
	if err != nil {
		return nil, fmt.Errorf("sextant: read key file: %w", err)
	}
	defer func() { _ = f.Close() }()

	var keys []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		keys = append(keys, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("sextant: read key file: %w", err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("sextant: key file %s has no keys", path)
	}
	return &FileKeys{keys: keys}, nil
}

// Next returns the next key, cycling.
//
// In order rather than at random, because the point of a file is reproducibility: two runs over the
// same list must check the same things in the same sequence.
func (f *FileKeys) Next() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := f.keys[f.next%len(f.keys)]
	f.next++
	return key, true
}

// Kind names this source.
func (f *FileKeys) Kind() KeySourceKind { return SourceFile }

// Degraded reports false: a declared list is exactly what was asked for.
func (f *FileKeys) Degraded() bool { return false }

// Len is how many keys the file held.
func (f *FileKeys) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.keys)
}

// ─── the cache's own keyspace ───────────────────────────────────────────────────

// ScanKeys enumerates keys from the cache itself.
//
// DEGRADED, and it reports itself as such. Two reasons, and the second is the serious one:
//
//  1. It enumerates CACHED keys rather than CHANGED rows. A key written a thousand times and never
//     cached cannot be sampled at all, and a key cached once and never written again is sampled
//     forever — so the sample is weighted away from the population a stale entry lives in.
//  2. It asks the cache what to check the cache on. A cache that dropped an invalidation also has
//     no entry to enumerate, so the very failure this exists to find can hide itself by looking
//     like an absent key.
//
// It is offered because a deployment with no replication access has nothing else, and a degraded
// measurement that says so beats no measurement. It must never be the default.
type ScanKeys struct {
	client redis.UniversalClient
	match  string
	strip  func(string) string

	mu     sync.Mutex
	buf    []string
	cursor uint64
	// wrapped records that a full pass has completed, which is what makes "we have seen the whole
	// keyspace" a fact rather than an assumption.
	wrapped bool
}

// ScanKeysOptions configures enumeration of a cache's keyspace.
type ScanKeysOptions struct {
	Client redis.UniversalClient

	// Match is the glob the cache's own keys match, normally the key template with `{key}`
	// replaced by `*`.
	Match string

	// Prefix and Suffix are stripped from a cache key to recover the verifier key. They are what
	// the key template put there.
	Prefix string
	Suffix string
}

// NewScanKeys builds a degraded key source over a cache's keyspace.
func NewScanKeys(opts ScanKeysOptions) (*ScanKeys, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("sextant: no cache client to scan")
	}
	if opts.Match == "" {
		return nil, fmt.Errorf("sextant: scanning needs a key pattern")
	}
	return &ScanKeys{
		client: opts.Client,
		match:  opts.Match,
		strip: func(k string) string {
			k = strings.TrimPrefix(k, opts.Prefix)
			return strings.TrimSuffix(k, opts.Suffix)
		},
	}, nil
}

// Fill advances the scan cursor, buffering the keys it found.
//
// Separate from Next because SCAN is a network round trip and Next is called inside the verifier's
// sampling loop; a source that blocked there would make the verifier's own latency a function of
// the cache it is checking.
func (s *ScanKeys) Fill(ctx context.Context) error {
	s.mu.Lock()
	cursor := s.cursor
	s.mu.Unlock()

	keys, next, err := s.client.Scan(ctx, cursor, s.match, 256).Result()
	if err != nil {
		return fmt.Errorf("sextant: scan: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		s.buf = append(s.buf, s.strip(k))
	}
	// Bounded: this runs beside a live system and must not grow with its keyspace.
	if len(s.buf) > 10_000 {
		s.buf = s.buf[len(s.buf)-10_000:]
	}
	s.cursor = next
	if next == 0 {
		s.wrapped = true
	}
	return nil
}

// Next returns a key at random from what has been scanned.
//
// Random rather than in scan order, so a key that is stale only intermittently is still reached: a
// fixed order would check the same keys at the same phase of every cycle.
func (s *ScanKeys) Next() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.buf) == 0 {
		return "", false
	}
	return s.buf[rand.IntN(len(s.buf))], true
}

// Kind names this source.
func (s *ScanKeys) Kind() KeySourceKind { return SourceScan }

// Degraded reports true, always. See the type's documentation.
func (s *ScanKeys) Degraded() bool { return true }

// CompletedAPass reports whether the cursor has wrapped at least once.
func (s *ScanKeys) CompletedAPass() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wrapped
}

// ─── the binlog ─────────────────────────────────────────────────────────────────

// Kind names RecentKeys as the binlog source.
//
// RecentKeys is what a replication tailer feeds: Touch is called for each changed row. The tailer
// itself lives outside this package because it is a different dependency, but the source it fills
// is the primary one and says so.
func (r *RecentKeys) Kind() KeySourceKind { return SourceBinlog }

// Degraded reports false: changed rows are the population a stale entry lives in.
func (r *RecentKeys) Degraded() bool { return false }
