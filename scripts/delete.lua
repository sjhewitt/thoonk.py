-- ARGS: feed, instance
if redis.call('srem', 'feeds', ARGV[1]) == 0 then
    return false
end
schema = {
  feed = function(name) return {
    'feed.config:'..name,
    'feed.ids:'..name,
    'feed.items:'..name,
    'feed.publishes:'..name
  } end,
  sortedfeed = function(name) return {
    'feed.config:'..name,
    'feed.ids:'..name,
    'feed.items:'..name,
    'feed.publishes:'..name,
    'feed.idincr:'..name
  } end,
  queue = function(name) return {
    'feed.config:'..name,
    'feed.ids:'..name,
    'feed.items:'..name,
    'feed.publishes:'..name
  } end,
  job = function(name) return {
    'feed.config:'..name,
    'feed.ids:'..name,
    'feed.items:'..name,
    'feed.publishes:'..name,
    'feed.published:'..name,
    'feed.claimed:'..name,
    'feed.cancelled:'..name,
    'feed.finishes:'..name,
    'feed.stalled:'..name
  } end
}
feedtype = redis.call('hget', 'feed.config:'..ARGV[1], 'type')
redis.call('del', unpack(schema[feedtype](ARGV[1])))
redis.call('publish', 'delfeed', ARGV[1]..'\0'..ARGV[2])
return true
