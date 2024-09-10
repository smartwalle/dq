-- KEYS[1] - 延迟队列
-- KEYS[2] - 待处理队列
-- KEYS[3] - MessageKeyPrefix
-- ARGV[1] - 单次处理数量

-- 获取当前时间
local time = redis.call('TIME')
local milliseconds = time[1] * 1000 + math.floor(time[2] / 1000)

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', milliseconds, 'LIMIT', 0, ARGV[1])
if (#ids > 0) then
    for _, id in ipairs(ids) do
        local mKey = KEYS[3]..id
        -- 判断消息结构是否存在
        local exists = redis.call('EXISTS', mKey)
        if (exists == 1) then
            local uuid = redis.call('HGET', mKey, 'uuid')
            if (uuid ~= nil and uuid ~= '') then
                local newKey = KEYS[3]..uuid
                redis.call('RPUSH', KEYS[2], newKey)
                redis.call('RENAME', mKey, newKey)
            end
        end
        redis.call('ZREM', KEYS[1], id)
    end
end