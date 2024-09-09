package dq

import "github.com/redis/go-redis/v9"

// 消息结构(hash)
// id -- 消息业务id
// uuid -- 消息唯一id
// qn -- 队列名称
// pl -- 消息内容
// dt -- 消息投递时间
// rc -- 剩余重试次数
// to -- 执行超时时间

// 延迟队列(sorted set) - member: 消息id，score: 消费时间
// 待处理队列(list) - element: 消息 uuid
// 处理中队列(sorted set) - member: 消息id, score: 确认处理成功超时时间
// 待重试队列(list) - element: 消息 uuid

// ScheduleScript 添加消息
// KEYS[1] - 延迟队列
// KEYS[2] - 消息结构
//
// ARGV[1] - 消息 id
// ARGV[2] - 消息 uuid
// ARGV[3] - 消费时间
// ARGV[4] - 队列名称
// ARGV[5] - 消息内容
// ARGV[6] - 剩余重试次数
// ARGV[7] - 执行超时时间
var ScheduleScript = redis.NewScript(`
-- 添加到[延迟队列]
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
-- 获取当前时间
local time = redis.call('TIME')
local timestamp = tonumber(time[1])
-- 写入消息结构
redis.call('HMSET', KEYS[2], 'id', ARGV[1], 'uuid', ARGV[2], 'qn', ARGV[4], 'pl', ARGV[5], 'rc', ARGV[6], 'to', ARGV[7], 'dt', timestamp)
`)

// RemoveScript 删除消息
// KEYS[1] - 延迟队列
// KEYS[2] - MessageKey(id)
// ARGV[1] - id
var RemoveScript = redis.NewScript(`
-- 从[延迟队列]删除
redis.call('ZREM', KEYS[1], ARGV[1])
-- 删除消息结构
redis.call('DEL', KEYS[2])
`)

// ScheduleToPendingScript 将消息从[延迟队列]转移到[待处理队列]
// KEYS[1] - 延迟队列
// KEYS[2] - 待处理队列
// KEYS[3] - MessageKeyPrefix
// ARGV[1] - 单次处理数量
var ScheduleToPendingScript = redis.NewScript(`
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
`)

// PendingToActiveScript 将消息从[待处理队列]转移到[处理中队列]
// KEYS[1] - 待处理队列
// KEYS[2] - 处理中队列
var PendingToActiveScript = redis.NewScript(`
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
`)

// ActiveToRetryScript 将[处理中队列]中已经消费超时的消息转移到[待重试队列]
// KEYS[1] - 处理中队列
// KEYS[2] - 待重试队列
var ActiveToRetryScript = redis.NewScript(`
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
`)

// AckScript 消费成功
// KEYS[1] - 处理中队列
// KEYS[2] - MessageKey(uuid)
var AckScript = redis.NewScript(`
local key = KEYS[2]
local score = redis.call('ZSCORE', KEYS[1], key)
if (not score) then
	return
end

-- 从[处理中队列]删除
redis.call('ZREM', KEYS[1], key)
-- 删除消息结构
redis.call('DEL', key)
`)

// NackScript 消费失败
// KEYS[1] - 处理中队列
// KEYS[2] - 待重试队列
// KEYS[3] - MessageKey(uuid)
var NackScript = redis.NewScript(`
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
`)

// RetryToAciveScript 将消息从[待重试队列]转移到[处理中队列]
// KEYS[1] - 待重试队列
// KEYS[2] - 处理中队列
var RetryToAciveScript = redis.NewScript(`
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
`)

// RenewActiveTimeoutScript 延长[处理中队列]消息的消费成功超时时间
// KEYS[1] - 处理中队列
// ARGV[1] - 消息 uuid
// ARGV[2] - 确认处理成功超时时间
var RenewActiveTimeoutScript = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if (not score) then
	return
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
`)
