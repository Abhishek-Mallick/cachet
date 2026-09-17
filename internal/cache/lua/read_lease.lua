-- read_lease.lua — read an entry, or take the lease to fill it.
--
-- KEYS[1] = entry key
-- KEYS[2] = lease key
-- ARGV[1] = lease token (opaque, unique per attempt)
-- ARGV[2] = lease TTL in milliseconds
--
-- Returns one of:
--   {1, rv, fv, payload, negative, tenant, status}   HIT      — serve this
--   {2}                                              GRANTED  — you fill it; nobody else will
--   {3}                                              WAIT     — someone else is filling it
--
-- WHY THIS IS ONE SCRIPT AND NOT TWO CALLS
--
-- Reading and then taking a lease in two round trips leaves a window in which every caller sees a
-- miss before any of them has taken the lease. Under a stampede that window IS the stampede: ten
-- thousand callers all read, all miss, and only then start competing for a lease that no longer
-- protects anything, because they have already decided to go to the database. Deciding both in one
-- atomic step is the entire mechanism.
--
-- WHY A LEASE AND NOT JUST DEDUPLICATION
--
-- The compare-and-set in fill_cas.lua already deduplicates: a slow fill cannot clobber a newer
-- value. That fixes ORDERING. It does nothing about ADMISSION — every one of those ten thousand
-- callers still reads the database, and the CAS merely sorts out whose answer wins afterwards. The
-- lease is what stops them being issued at all.

local e = redis.call('HMGET', KEYS[1], 'v', 'f', 'p', 'n', 'd', 's')

-- A tombstoned entry has had 'v' removed, so it falls through to the lease path exactly like an
-- absent one. That is deliberate: an invalidated hot key is when a stampede is MOST likely, because
-- the invalidation and the traffic peak are usually the same event.
if e[1] then
  return {1, e[1], e[2], e[3], e[4], e[5], e[6]}
end

-- SET NX is the whole admission decision: exactly one caller can win it, and PX guarantees the key
-- becomes leasable again even if that caller dies without ever filling. Without the expiry, a
-- holder that crashed mid-fill would make the key permanently unfillable and quietly downgrade
-- every reader of it to a database read, forever.
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return {2}
end

return {3}
