-- KEYS[1] = processed_set_key (e.g., "proc:mystream")
-- ARGV[1] = stream
-- ARGV[2] = group
-- ARGV[3] = entry_id
-- Returns: 1 if newly marked processed and acked, 0 if already processed (still acked)

local processed = KEYS[1]
local stream = ARGV[1]
local group = ARGV[2]
local entry_id = ARGV[3]

if redis.call('SISMEMBER', processed, entry_id) == 1 then
  redis.call('XACK', stream, group, entry_id)
  return 0
end

redis.call('SADD', processed, entry_id)
redis.call('XACK', stream, group, entry_id)
return 1

