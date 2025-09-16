-- KEYS[1] = lock key
-- ARGV[1] = owner token
-- Return: 1 if deleted, 0 otherwise
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
