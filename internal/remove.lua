-- KEYS[1] - 延迟队列
-- KEYS[2] - MessageKey(id)
-- ARGV[1] - id

-- 从[延迟队列]删除
redis.call('ZREM', KEYS[1], ARGV[1])
-- 删除消息结构
redis.call('DEL', KEYS[2])