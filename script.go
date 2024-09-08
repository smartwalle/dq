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
// KEYS[2] - MessageKeyPrefix
// ARGV[1] - 消息 id
var RemoveScript = redis.NewScript(`
-- 从[延迟队列]删除
redis.call('ZREM', KEYS[1], ARGV[1])
-- 删除消息结构
local key = KEYS[2]..ARGV[1]
redis.call('DEL', key)
`)

// ScheduleToPendingScript 将消息从[延迟队列]转移到[待处理队列]
// KEYS[1] - 延迟队列
// KEYS[2] - 待处理队列
// KEYS[3] - MessageKeyPrefix
// ARGV[1] - 单次处理数量
var ScheduleToPendingScript = redis.NewScript(`
-- 获取当前时间
local time = redis.call('TIME')
local timestamp = tonumber(time[1])

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', timestamp, 'LIMIT', 0, ARGV[1])
if (#ids > 0) then
    for _, id in ipairs(ids) do
        local k1 = KEYS[3]..id
        local uuid = redis.call('HGET', k1, 'uuid')
		if (uuid ~= nil and uuid ~= '') then
			local k2 = KEYS[3]..uuid
        	redis.call('RPUSH', KEYS[2], uuid)
			redis.call('RENAME', k1, k2)
		end
        redis.call('ZREM', KEYS[1], id)
    end
end
`)

// PendingToActiveScript 将消息从[待处理队列]转移到[处理中队列]
// KEYS[1] - 待处理队列
// KEYS[2] - 处理中队列
// KEYS[3] - MessageKeyPrefix
var PendingToActiveScript = redis.NewScript(`
local uuid = redis.call('LPOP', KEYS[1])
if (not uuid) then
    return ''
end
local key = KEYS[3]..uuid
-- 获取消息的执行超时时间
local timeout = redis.call('HGET', key, 'to')
redis.call('ZADD', KEYS[2], timeout, uuid)
return uuid
`)

// ActiveToRetryScript 将[处理中队列]中已经消费超时的消息转移到[待重试队列]
// KEYS[1] - 处理中队列
// KEYS[2] - 待重试队列
// KEYS[3] - MessageKeyPrefix
var ActiveToRetryScript = redis.NewScript(`
local doRetry = function(uuid)
    local key = KEYS[3]..uuid

    -- 获取剩余重试次数
    local count = redis.call('HGET', key, 'rc')
    if (count ~= nil and count ~= '' and count ~= false and tonumber(count) > 0) then
        -- 剩余重试次数大于 0
        -- 更新剩余重试次数
        redis.call('HINCRBY', key, 'rc', -1)
        -- 添加到[待重试队列]中
        redis.call('RPUSH', KEYS[2], uuid)
    else
        -- 删除[消息结构]
        redis.call('DEL', key)
		-- TODO 记录失败消息
    end
	-- 从[处理中队列]中删除消息
	redis.call('ZREM', KEYS[1], uuid)
end

-- 获取当前时间
local time = redis.call('TIME')
local timestamp = tonumber(time[1])

-- 获取[处理中队列]中已经消费超时的消息
local uuids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', timestamp)
if (#uuids > 0) then
    for _, uuid in ipairs(uuids) do
		if (uuid ~= nil and uuid ~= '') then
			-- 重试处理逻辑
        	doRetry(uuid)
		end
    end
end
`)

// AckScript 消费成功
// KEYS[1] - 处理中队列
// KEYS[2] - MessageKeyPrefix
// ARGV[1] - 消息 uuid
var AckScript = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if (not score) then
	return
end

-- 从[处理中队列]删除
redis.call('ZREM', KEYS[1], ARGV[1])
-- 删除消息结构
local key = KEYS[2]..ARGV[1]
redis.call('DEL', key)
`)

// NackScript 消费失败
// KEYS[1] - 处理中队列
// KEYS[2] - 待重试队列
// KEYS[3] - MessageKeyPrefix
// ARGV[1] - 消息 uuid
var NackScript = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if (not score) then
	return 
end

local key = KEYS[3]..ARGV[1]

-- 获取剩余重试次数
local count = redis.call('HGET', key, 'rc')
if count ~= nil and count ~= '' and count ~= false and tonumber(count) > 0 then
    -- 剩余重试次数大于 0
    -- 更新剩余重试次数
    redis.call('HINCRBY', key, 'rc', -1)
    -- 添加到[待重试队列]中
    redis.call('RPUSH', KEYS[2], ARGV[1])
else
    -- 删除[消息结构]
    redis.call('DEL', key)
end
-- 从[处理中队列]中删除消息
redis.call('ZREM', KEYS[1], ARGV[1])
`)

// RetryToAciveScript 将消息从[待重试队列]转移到[处理中队列]
// KEYS[1] - 待重试队列
// KEYS[2] - 处理中队列
// KEYS[3] - MessageKeyPrefix
var RetryToAciveScript = redis.NewScript(`
local uuid = redis.call('LPOP', KEYS[1])
if (not uuid) then
    return ''
end
local key = KEYS[3]..uuid
-- 获取消息的执行超时时间
local timeout = redis.call('HGET', key, 'to')
redis.call('ZADD', KEYS[2], timeout, uuid)
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
