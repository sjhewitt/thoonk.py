-- ARGV: feed, id
if redis.call('zrem', 'feed.ids:'..ARGV[1], ARGV[2]) == 0 then
    return false
end
redis.call('hdel', 'feed.items:'..ARGV[1], ARGV[2])
redis.call('publish', 'feed.retract:'..ARGV[1], ARGV[2])
return true

