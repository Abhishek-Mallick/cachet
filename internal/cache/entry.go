// Package cache is Cachet's cache-side data path.
//
// Phase 1 is deliberately naive: cache-aside with a TTL and no invalidation. That is not an
// oversight — it exists to produce a staleness number that is knowingly bad, because a number is
// the only honest way to motivate the exact-invalidation work in Phase 2
// (docs/cachet-benchmarking.md §9).
package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// encodingV1 tags the current entry layout.
//
// Entries outlive deploys, so the layout carries its own version. An old process meeting a new
// encoding must refuse it rather than misread the bytes as versions — that would corrupt the
// compare-and-set the entire consistency model rests on (CONSISTENCY.md §2).
const encodingV1 byte = 1

// headerBytes is the fixed prefix: one version byte plus two uint64 versions.
const headerBytes = 1 + 8 + 8

// ErrCorruptEntry reports an entry that cannot be decoded.
//
// It is a distinct error rather than a decode failure folded into "miss", because a corrupt entry
// and an absent one call for different responses: a miss is normal, corruption is a signal.
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
	Payload     []byte
}

// Encode serialises the entry.
//
// The layout is fixed-width and hand-rolled rather than JSON or protobuf: this runs on every cache
// hit, and a hit's entire latency budget is a few hundred microseconds. Parsing is a slice and two
// integer loads.
func (e Entry) Encode() []byte {
	buf := make([]byte, headerBytes+len(e.Payload))
	buf[0] = encodingV1
	binary.BigEndian.PutUint64(buf[1:9], e.RowVersion)
	binary.BigEndian.PutUint64(buf[9:17], e.FillVersion)
	copy(buf[headerBytes:], e.Payload)
	return buf
}

// Decode parses an entry, rejecting anything it does not fully understand.
func Decode(b []byte) (Entry, error) {
	if len(b) < headerBytes {
		return Entry{}, fmt.Errorf("%w: %d bytes, need at least %d", ErrCorruptEntry, len(b), headerBytes)
	}
	if b[0] != encodingV1 {
		return Entry{}, fmt.Errorf("%w: unknown encoding version %d", ErrCorruptEntry, b[0])
	}

	// The payload is copied rather than aliased: the caller keeps it well past the lifetime of the
	// buffer the client hands us, and aliasing a pooled read buffer is the kind of bug that shows
	// up much later as one row's payload appearing under another row's key.
	payload := make([]byte, len(b)-headerBytes)
	copy(payload, b[headerBytes:])

	return Entry{
		RowVersion:  binary.BigEndian.Uint64(b[1:9]),
		FillVersion: binary.BigEndian.Uint64(b[9:17]),
		Payload:     payload,
	}, nil
}
