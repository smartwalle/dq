 -- KEYS[1] - 待消费队列
 -- KEYS[2] - 消息结构
 -- ARGV[1] - 消息 id (id)
 -- ARGV[2] - 消息 uuid (uuid)
 -- ARGV[3] - 消费时间
 -- ARGV[4] - 队列名称 (qn)
 -- ARGV[5] - 消息内容 (pl)
 -- ARGV[6] - 剩余重试次数 (rc)
 -- 投递时间 (dt)

-- 添加到[待消费队列]
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
-- 获取当前时间
local now = redis.call('TIME')
local second = tonumber(now[1])
-- 写入消息结构
redis.call('HMSET', KEYS[2], 'id', ARGV[1], 'uuid', ARGV[2], 'qn', ARGV[4], 'pl', ARGV[5], 'rc', ARGV[6], 'dt', second)