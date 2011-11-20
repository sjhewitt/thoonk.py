-- ARGV: name, config(json), instance
if redis.call('sismember', 'feeds', name) then
    return false
end
config = cjson.decode(ARGV[2])
feed = 'feed.config:'..ARGV[1]
table.foreach(config, function(k, v)
  redis.call('hset', feed, k, v)
end)
redis.call('publish', 'conffeed', ARGV[1]..'\0'..ARGV[3])
return true
