-- fill_cas.lua — versioned, compare-and-set fill.
--
-- KEYS[1] = entry key
-- KEYS[2] = lease key
-- ARGV[1] = row version    (zero-padded decimal string, see the note below)
-- ARGV[2] = fill version   (same encoding)
-- ARGV[3] = encoded row
-- ARGV[4] = "1" for a negative entry ("this row does not exist"), "0" otherwise
-- ARGV[5] = TTL in milliseconds
-- ARGV[6] = schema fingerprint
-- ARGV[7] = lease token held by this filler, or "" if it holds none
--
-- The row is stored whole so that a cache hit returns the SAME record as a cache miss. It is opaque
-- here: this script never looks inside it, which is why an arbitrary user table needs no change to
-- the compare-and-set below. The fingerprint travels with it so a reader expecting a different
-- shape treats the entry as a miss rather than decoding it into the wrong columns.
--
-- Returns 1 if the fill was applied, 0 if it lost the compare-and-set.
--
-- Either way the lease is released, because either way this caller has finished the work the lease
-- was protecting. Holding it through a LOST compare-and-set would stall the key for the rest of the
-- interval while a perfectly good newer value was already sitting there.
--
-- ON VERSION ENCODING: versions arrive as fixed-width zero-padded decimal STRINGS and are compared
-- with Lua's string operators. They are never passed through tonumber(). Lua numbers are IEEE
-- doubles with a 53-bit mantissa, while an HLC version is a full uint64 — tonumber() silently
-- rounds, so two adjacent versions can compare equal. That would let a stale fill win a
-- compare-and-set, which is the precise failure this whole file exists to prevent. Fixed-width
-- zero padding makes lexicographic order identical to numeric order, exactly.

-- releaseLease hands the lease back, but ONLY if this caller still owns it.
--
-- Checking the token is not defensive programming, it is the correctness of the lease. A filler
-- whose lease expired mid-fill no longer owns the key; by the time its write lands, another caller
-- may hold the lease and be filling. Releasing blindly would drop that second holder's lease and
-- admit a third filler — reopening the stampede the lease had just closed, at the worst moment.
local function releaseLease()
  if ARGV[7] == nil or ARGV[7] == '' then
    return
  end
  if redis.call('GET', KEYS[2]) == ARGV[7] then
    redis.call('DEL', KEYS[2])
  end
end

local cur = redis.call('HMGET', KEYS[1], 'v', 'f', 't')
local rv, fv, tv = cur[1], cur[2], cur[3]

-- Resurrection guard. A fill carrying a value older than a known invalidation must be rejected
-- rather than served: the tombstone is evidence that a newer write exists, even though this reader
-- did not see it (CONSISTENCY.md §2).
if tv and ARGV[1] < tv then
  releaseLease()
  return 0
end

if rv then
  -- A slow read's fill must never clobber a newer write's value.
  if ARGV[1] < rv then
    releaseLease()
    return 0
  end
  -- Same row version: only a fresher SNAPSHOT is worth writing. This is what lets a re-read refresh
  -- an entry's freshness without the row having changed — the reason fill version is tracked
  -- separately from row version at all.
  if ARGV[1] == rv and ARGV[2] <= fv then
    releaseLease()
    return 0
  end
end

redis.call('HSET', KEYS[1], 'v', ARGV[1], 'f', ARGV[2], 'r', ARGV[3], 'n', ARGV[4], 'h', ARGV[6])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
releaseLease()
return 1
