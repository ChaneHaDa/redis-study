-- KEYS[1] = lock key
-- ARGV[1] = owner token
-- ARGV[2] = ttl_ms (string/number)
-- Return: 1 if ttl updated, 0 otherwise
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0

