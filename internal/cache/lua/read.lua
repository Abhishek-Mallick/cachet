-- read.lua — read one entry.
--
-- KEYS[1] = entry key
--
-- Returns {row_version, fill_version, payload, negative_flag, tenant_id, status} on a hit, or an
-- empty table on a miss.
--
-- The row fields are returned because a cache hit and a cache miss must produce the same record. An
-- entry that carried only the payload would make a caller's view of a row depend on whether the
-- cache was warm.
--
-- A tombstoned entry reads as a miss: the tombstone removes 'v', so there is nothing to serve. The
-- marker itself is invisible to readers and matters only to the compare-and-set in fill_cas.lua.

local e = redis.call('HMGET', KEYS[1], 'v', 'f', 'p', 'n', 'd', 's')
if not e[1] then
  return {}
end
return {e[1], e[2], e[3], e[4], e[5], e[6]}
