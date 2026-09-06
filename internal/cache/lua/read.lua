-- read.lua — read one entry.
--
-- KEYS[1] = entry key
--
-- Returns {row_version, fill_version, payload, negative_flag} on a hit, or an empty table on a miss.
--
-- A tombstoned entry reads as a miss: the tombstone removes 'v', so there is nothing to serve. The
-- marker itself is invisible to readers and matters only to the compare-and-set in fill_cas.lua.

local e = redis.call('HMGET', KEYS[1], 'v', 'f', 'p', 'n')
if not e[1] then
  return {}
end
return {e[1], e[2], e[3], e[4]}
