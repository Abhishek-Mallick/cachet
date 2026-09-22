// Package cache is Cachet's cache-side data path.
//
// Every mutation of an entry is a compare-and-set against a version, and a lower version never
// overwrites a higher one (CONSISTENCY.md §2). That single rule is what makes racing fills,
// out-of-order invalidation, duplicate CDC delivery and tailer restarts all safe — and it is
// enforced in Lua, inside the cache server, because the read-modify-write has to be atomic with
// respect to every other engine instance.
package cache

import "errors"

// ErrCorruptEntry reports an entry the client cannot make sense of.
//
// It is a distinct error rather than being folded into "miss", because a corrupt entry and an absent
// one call for different responses: a miss is normal, corruption is a signal that something is
// writing to these keys other than Cachet.
var ErrCorruptEntry = errors.New("cache: corrupt entry")

// Entry is one cached row.
//
// Two versions, and the distinction is load-bearing (CONSISTENCY.md §1):
//
//   - RowVersion orders fills against each other, so a slow read cannot clobber a newer write.
//   - FillVersion answers "is this entry fresh enough for this session?", which is a different
//     question. A row untouched for a year has an ancient RowVersion and may still have been read
//     from the database a millisecond ago.
//
// Watermarking on FillVersion rather than RowVersion is what keeps SESSION from collapsing the hit
// rate on any shard that takes writes.
type Entry struct {
	RowVersion  uint64
	FillVersion uint64

	// Row is the whole row, encoded by the schema package, and opaque here.
	//
	// The entry carries the WHOLE row because a cache hit and a cache miss must return the same
	// record. An earlier version cached only the payload on the reasoning that nothing read the
	// other columns on the hot path — which was true until conditional writes made status
	// meaningful, and then the same key returned a different record depending on whether the cache
	// happened to be warm. That is a correctness bug wearing a performance costume.
	//
	// This layer does not decode it. Keeping the cache schema-agnostic is what lets an arbitrary
	// user table need no change here and none in the Lua.
	Row []byte

	// Fingerprint identifies the SHAPE of Row.
	//
	// An entry whose fingerprint differs from the reader's reads as a miss — enforced in the Lua,
	// so a mismatch takes the lease path rather than being discovered afterwards by a caller who
	// then goes to the origin unprotected. That is what makes a schema change safe with no flush:
	// old entries stop being served, and two engines mid-deploy miss each other rather than
	// misread each other.
	Fingerprint string

	// Negative marks "this row does not exist", which is a cacheable fact rather than an absence of
	// one. Without it every lookup of a missing row is a database query, and a workload that probes
	// for absent keys bypasses the cache entirely. An insert must invalidate it, which is what
	// gives read-own-inserts.
	Negative bool
}
