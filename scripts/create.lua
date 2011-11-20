-- ARGV: name, config, instance
if redis.call('sadd', 'feeds', ARGV[1]) == 0 then
    -- feed already exists
    return false
end
feed = 'feed.config:'..ARGV[1]
config = cjson.decode(ARGV[2])
-- TODO: check if config has a type key
for k, v in pairs(config) do
  redis.call('hset', feed, k, v)
end
redis.call('publish', 'newfeed', ARGV[1]..'\0'..ARGV[3])
return true
