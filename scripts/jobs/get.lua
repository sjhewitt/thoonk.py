-- ARGV: name, id, time
r1 = redis.call('zadd', 'feed.claimed:'..ARGV[1], ARGV[3], ARGV[2]);
r2 = redis.call('hget', 'feed.items:'..ARGV[1], ARGV[2]);
return {r1, r2}
