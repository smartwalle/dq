-- KEYS[1] - 处理中队列
-- KEYS[2] - 待重试队列
-- KEYS[3] - MessageKey(uuid)

local key = KEYS[3]
local score = redis.call('ZSCORE', KEYS[1], key)
if (not score) then
    return ''
end

-- 判断消息结构是否存在
local exists = redis.call('EXISTS', key)
if (exists == 0) then
    return
end

-- 获取剩余重试次数
local count = redis.call('HGET', key, 'rc')
if count ~= nil and count ~= '' and count ~= false and tonumber(count) > 0 then
    -- 剩余重试次数大于 0
    -- 更新剩余重试次数
    redis.call('HINCRBY', key, 'rc', -1)
    -- 添加到[待重试队列]中
    redis.call('RPUSH', KEYS[2], key)
else
    -- 删除[消息结构]
    redis.call('DEL', key)
end
-- 从[处理中队列]中删除消息
redis.call('ZREM', KEYS[1], key)