-- fill_cas.lua — versioned, compare-and-set fill.
--
-- KEYS[1] = entry key
-- ARGV[1] = row version    (zero-padded decimal string, see the note below)
-- ARGV[2] = fill version   (same encoding)
-- ARGV[3] = payload
-- ARGV[4] = "1" for a negative entry ("this row does not exist"), "0" otherwise
-- ARGV[5] = TTL in milliseconds
--
-- Returns 1 if the fill was applied, 0 if it lost the compare-and-set.
--
-- ON VERSION ENCODING: versions arrive as fixed-width zero-padded decimal STRINGS and are compared
-- with Lua's string operators. They are never passed through tonumber(). Lua numbers are IEEE
-- doubles with a 53-bit mantissa, while an HLC version is a full uint64 — tonumber() silently
-- rounds, so two adjacent versions can compare equal. That would let a stale fill win a
-- compare-and-set, which is the precise failure this whole file exists to prevent. Fixed-width
-- zero padding makes lexicographic order identical to numeric order, exactly.

local cur = redis.call('HMGET', KEYS[1], 'v', 'f', 't')
local rv, fv, tv = cur[1], cur[2], cur[3]

-- Resurrection guard. A fill carrying a value older than a known invalidation must be rejected
-- rather than served: the tombstone is evidence that a newer write exists, even though this reader
-- did not see it (CONSISTENCY.md §2).
if tv and ARGV[1] < tv then
  return 0
end

if rv then
  -- A slow read's fill must never clobber a newer write's value.
  if ARGV[1] < rv then
    return 0
  end
  -- Same row version: only a fresher SNAPSHOT is worth writing. This is what lets a re-read refresh
  -- an entry's freshness without the row having changed — the reason fill version is tracked
  -- separately from row version at all.
  if ARGV[1] == rv and ARGV[2] <= fv then
    return 0
  end
end

redis.call('HSET', KEYS[1], 'v', ARGV[1], 'f', ARGV[2], 'p', ARGV[3], 'n', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1
