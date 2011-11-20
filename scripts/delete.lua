-- ARGS: feed, instance
if redis.call('srem', 'feeds', ARGV[1]) == 0 then
    return false
end
feedtype = redis.call('hget', 'feed.config:'..ARGV[1], 'type')
-- TODO: delete schema keys!
redis.call('del', 'feed.config:'..ARGV[1])
redis.call('publish', 'delfeed', ARGV[1]..'\0'..ARGV[2])
return true
