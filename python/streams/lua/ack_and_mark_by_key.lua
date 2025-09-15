-- KEYS[1] = processed_by_key_set (e.g., "proc-key:mystream:order_id")
-- ARGV[1] = stream
-- ARGV[2] = group
-- ARGV[3] = entry_id
-- ARGV[4] = dedup_value (e.g., order_id)
-- Returns: 1 if newly marked processed for key and acked, 0 if already processed (still acked)

local processed_by_key = KEYS[1]
local stream = ARGV[1]
local group = ARGV[2]
local entry_id = ARGV[3]
local keyval = ARGV[4]

if redis.call('SISMEMBER', processed_by_key, keyval) == 1 then
  redis.call('XACK', stream, group, entry_id)
  return 0
end

redis.call('SADD', processed_by_key, keyval)
redis.call('XACK', stream, group, entry_id)
return 1

