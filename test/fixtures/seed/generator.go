// Command seed populates the Cachet test shards with deterministic fixture data.
//
// Determinism is the point. A consistency violation has to be reproducible from a test name alone
// (build plan §9.2), which is only true if the data underneath it is byte-identical on every run —
// so the generator is a pure function of (profile, seed) and never touches wall time, hostnames,
// or the global random source.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math/rand"
	"sort"
	"strconv"

	"github.com/Abhishek-Mallick/cachet/internal/storage"
)

// errStop is returned by tests to abort generation partway through.
var errStop = errors.New("seed: stopped")

// Profile describes one fixture size.
type Profile struct {
	Name         string
	Rows         uint64
	Seed         int64
	Tenants      uint32
	PayloadBytes int
}

// profiles are the fixture sizes the harness and the benchmarks share.
//
// The three sizes exist for three different jobs: small keeps the e2e suite fast enough that people
// keep running it, medium is large enough that the working set stops fitting in the cache, and
// large is the one the published benchmark numbers come from.
var profiles = map[string]Profile{
	"small":  {Name: "small", Rows: 10_000, Seed: 20260905, Tenants: 16, PayloadBytes: 256},
	"medium": {Name: "medium", Rows: 1_000_000, Seed: 20260905, Tenants: 64, PayloadBytes: 256},
	"large":  {Name: "large", Rows: 10_000_000, Seed: 20260905, Tenants: 256, PayloadBytes: 256},
}

// ProfileByName looks up a fixture profile, failing fast on an unknown name rather than silently
// seeding something other than what was asked for.
func ProfileByName(name string) (Profile, error) {
	p, ok := profiles[name]
	if !ok {
		names := make([]string, 0, len(profiles))
		for n := range profiles {
			names = append(names, n)
		}
		sort.Strings(names)
		return Profile{}, fmt.Errorf("seed: unknown profile %q (have %v)", name, names)
	}
	return p, nil
}

// CacheKey returns the cache key for a row id. Routing is performed on this string, so the format
// is part of the contract between the seeder, the engine, and the benchmark harness.
func CacheKey(id uint64) string { return "entities:" + strconv.FormatUint(id, 10) }

// Generate produces the profile's rows in a fixed order, handing each to yield.
//
// Rows are streamed rather than returned as a slice: the large profile is ten million rows, and
// materialising it would cost gigabytes for no benefit — the loader batches as it goes.
//
// Generation stops at the first yield error so that a failing insert batch aborts the seed rather
// than leaving a shard half-populated behind a checksum that claims otherwise.
func Generate(p Profile, yield func(storage.Record) error) error {
	if p.Rows == 0 {
		return nil
	}
	if p.Tenants == 0 {
		return errors.New("seed: profile must have at least one tenant")
	}
	if p.PayloadBytes < 0 {
		return errors.New("seed: negative payload size")
	}

	// math/rand, not crypto/rand: determinism IS the requirement here, and a CSPRNG would defeat
	// the entire purpose of the fixture. gosec's G404 is excluded for this reason in .golangci.yml.
	rng := rand.New(rand.NewSource(p.Seed))

	payload := make([]byte, p.PayloadBytes)
	for id := uint64(1); id <= p.Rows; id++ {
		// Read the random bytes into a scratch buffer, then hand the yield a copy: the loader may
		// retain the record past the call, and aliasing one buffer across ten million rows is the
		// kind of bug that only shows up as corrupted fixture data much later.
		rng.Read(payload)
		row := make([]byte, p.PayloadBytes)
		copy(row, payload)

		rec := storage.Record{
			ID:       id,
			TenantID: uint32(rng.Int31n(int32(p.Tenants))),
			Status:   uint8(rng.Int31n(4)),
			Payload:  row,
		}
		if err := yield(rec); err != nil {
			return fmt.Errorf("seed: row %d: %w", id, err)
		}
	}
	return nil
}

// Checksum returns a stable digest of a set of records, used to prove two seed runs produced
// identical data without comparing every row by hand.
func Checksum(recs []storage.Record) string {
	h := sha256.New()
	for _, r := range recs {
		HashRecord(h, r)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// HashRecord folds one record into a running digest. The loader hashes per shard as it streams,
// and Checksum hashes a materialised slice; both go through here so the two can never disagree
// about field order and silently report mismatched checksums for identical data.
func HashRecord(h hash.Hash, r storage.Record) {
	var scratch [8]byte
	binary.BigEndian.PutUint64(scratch[:], r.ID)
	_, _ = h.Write(scratch[:])
	binary.BigEndian.PutUint32(scratch[:4], r.TenantID)
	_, _ = h.Write(scratch[:4])
	_, _ = h.Write([]byte{r.Status})
	_, _ = h.Write(r.Payload)
}
