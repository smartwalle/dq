-- KEYS[1] - 处理中队列
-- KEYS[2] - 待重试队列

local doRetry = function(key)
    -- 判断消息结构是否存在
    local exists = redis.call('EXISTS', key)
    if (exists == 0) then
        return
    end

    -- 获取剩余重试次数
    local count = redis.call('HGET', key, 'rc')
    if (count ~= nil and count ~= '' and count ~= false and tonumber(count) > 0) then
        -- 剩余重试次数大于 0
        -- 更新剩余重试次数
        redis.call('HINCRBY', key, 'rc', -1)
        -- 添加到[待重试队列]中
        redis.call('RPUSH', KEYS[2], key)
    else
        -- 删除[消息结构]
        redis.call('DEL', key)
        -- TODO 记录失败消息
    end
    -- 从[处理中队列]中删除消息
    redis.call('ZREM', KEYS[1], key)
end

-- 获取当前时间
local time = redis.call('TIME')
local milliseconds = time[1] * 1000 + math.floor(time[2] / 1000)

-- 获取[处理中队列]中已经消费超时的消息
local keys = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', milliseconds)
if (#keys > 0) then
    for _, key in ipairs(keys) do
        if (key ~= nil and key ~= '') then
            -- 重试处理逻辑
            doRetry(key)
        end
    end
end