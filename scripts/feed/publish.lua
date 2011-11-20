-- ARGV: feed, id, item, time
max = redis.call("hget", "feed.config:"..ARGV[1], "max_length")
if max and tonumber(max) > 0 then
    ids = redis.call('zrange', 'feed.ids:'..ARGV[1], 0, -tonumber(max))
    table.foreach(ids, function(i, id) 
        redis.call('zrem', 'feed.ids:'..ARGV[1], id)
        redis.call('hdel', 'feed.items:'..ARGV[1], id)
        redis.call('publish', 'feed.retract:'..ARGV[1], id) 
    end)
end

redis.call('incr', 'feed.publishes:'..ARGV[1])
redis.call('hset', 'feed.items:'..ARGV[1], ARGV[2], ARGV[3])
if redis.call('zadd', 'feed.ids:'..ARGV[1], ARGV[4], ARGV[2]) == 1 then
    redis.call('publish', 'feed.edit:'..ARGV[1], ARGV[2]..'\0'..ARGV[3])
else
    redis.call('publish', 'feed.publish:'..ARGV[1], ARGV[2]..'\0'..ARGV[3])
end
return zadd
