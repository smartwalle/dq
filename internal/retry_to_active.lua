-- KEYS[1] - 待重试队列
-- KEYS[2] - 处理中队列

local key = redis.call('LPOP', KEYS[1])
if (not key) then
    return ''
end
-- 判断消息结构是否存在
local exists = redis.call('EXISTS', key)
if (exists == 0) then
    return ''
end

-- 获取当前时间
local time = redis.call('TIME')
local milliseconds = time[1] * 1000 + math.floor(time[2] / 1000)
-- 获取消息 uuid
local uuid = redis.call('HGET', key, 'uuid')
-- 计算消息的执行超时时间
local timeout = milliseconds + (redis.call('HGET', key, 'to') * 1000)
redis.call('ZADD', KEYS[2], timeout, key)
return uuid