-- read.lua — read one entry.
--
-- KEYS[1] = entry key
-- ARGV[1] = the reader's schema fingerprint
--
-- Returns {row_version, fill_version, row, negative_flag} on a hit, or an empty table on a miss.
--
-- The whole row is returned because a cache hit and a cache miss must produce the same record. An
-- entry carrying less than the row would make a caller's view depend on whether the cache was warm.
--
-- AN ENTRY WRITTEN FOR A DIFFERENT SCHEMA READS AS A MISS. The fingerprint covers the row's shape,
-- so a mismatch means these bytes decode into a different set of columns than this reader expects.
-- Refusing here is what makes a schema change safe without flushing anything: old entries simply
-- stop being served, and two engines mid-deploy miss each other rather than misread each other.
--
-- A tombstoned entry reads as a miss: the tombstone removes 'v', so there is nothing to serve. The
-- marker itself is invisible to readers and matters only to the compare-and-set in fill_cas.lua.

local e = redis.call('HMGET', KEYS[1], 'v', 'f', 'r', 'n', 'h')
if not e[1] then
  return {}
end
if e[5] ~= ARGV[1] then
  return {}
end
return {e[1], e[2], e[3], e[4]}
