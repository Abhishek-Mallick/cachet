-- tombstone_cas.lua — versioned invalidation marker.
--
-- KEYS[1] = entry key
-- ARGV[1] = version of the write that invalidated this row (zero-padded decimal string)
-- ARGV[2] = TTL in milliseconds
--
-- Returns 1 if the tombstone was applied, 0 if it lost the compare-and-set.
--
-- This writes a MARKER rather than deleting the key, and that distinction is the whole point. A
-- plain DEL loses the delete-versus-fill race: a slow read that started before the write can land
-- afterwards and refill the old value, with nothing left behind to say it should not. The marker
-- survives to reject exactly that fill (product spec §6, Tier 0).
--
-- Because the rule is a compare-and-set rather than an unconditional write, applying the same
-- tombstone twice is a no-op. That is what makes binlog replay safe: the CDC tailer needs no
-- exactly-once delivery, because duplicate delivery cannot corrupt anything.

local cur = redis.call('HMGET', KEYS[1], 'v', 't')
local rv, tv = cur[1], cur[2]

-- An invalidation no newer than the value already present tells us nothing we do not know.
if rv and ARGV[1] <= rv then
  return 0
end
if tv and ARGV[1] <= tv then
  return 0
end

-- The payload fields go; the version marker stays. A reader sees no 'v' and treats the entry as a
-- miss, while a late fill still meets the marker and loses.
redis.call('HSET', KEYS[1], 't', ARGV[1])
redis.call('HDEL', KEYS[1], 'v', 'f', 'p', 'n')
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
