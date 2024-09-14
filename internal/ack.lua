-- KEYS[1] - 处理中队列
-- KEYS[2] - MessageKey(uuid)

local key = KEYS[2]
local score = redis.call('ZSCORE', KEYS[1], key)
if (not score) then
    return ''
end

-- 获取消息 uuid
local uuid = redis.call('HGET', key, 'uuid')
-- 从[处理中队列]删除
redis.call('ZREM', KEYS[1], key)
-- 删除消息结构
redis.call('DEL', key)
return uuid