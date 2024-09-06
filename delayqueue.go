package dq

import (
	"github.com/redis/go-redis/v9"
)

// 消息结构
// Key -- 消息唯一标识，ID
// Payload -- 消息内容

// 消息结构 - hash
// 待处理队列 - sorted set
// 待消费队列 - list
// 消费中队列 - sorted set
// 待重试队列 - list
// 剩余重试次数结构 - hash

// S0 添加消息
// KEYS[1] - 待处理队列
// KEYS[2] - 剩余重试次数结构
// ARGV[1] - 消息 id
// ARGV[2] - 消费时间
// ARGV[3] - 最大重试次数
var S0 = redis.NewScript(`
-- 添加到[待处理队列]
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
-- 写入剩余重试次数
redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
`)

// S1 将消息从[待处理队列]转移到[待消费队列]
// KEYS[1] - 待处理队列
// KEYS[2] - 待消费队列
// ARGV[1] - 消费时间
// ARGV[2] - 单次处理数量
var S1 = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if (#ids > 0) then
	redis.call('RPUSH', KEYS[2], unpack(ids))
    redis.call('ZREM', KEYS[1], unpack(ids))
end
return ids
`)

// S2 将消息从[待消费队列]转移到[消费中队列]
// KEYS[1] - 待消费队列
// KEYS[2] - 消费中队列
// ARGV[1] - 确认消费成功超时时间
var S2 = redis.NewScript(`
local id = redis.call('LPOP', KEYS[1])
if (not id) then
    return
end
redis.call('ZADD', KEYS[2], ARGV[1], id)
return msg
`)

// S3 将消息从[待重试队列]转移到[消费中队列]
// KEYS[1] - 待重试队列
// KEYS[2] - 消费中队列
// ARGV[1] - 确认消费成功超时时间
var S3 = redis.NewScript(`
local id = redis.call('LPOP', KEYS[1])
if (not id) then
    return
end
redis.call('ZADD', KEYS[2], ARGV[1], id)
return msg
`)

// S4 延长消费成功超时时间
// KEYS[1] - 消费中队列
// ARGV[1] - 消息 id
// ARGV[2] - 确认消费成功超时时间
var S4 = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if (not score) then
	return
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
`)

// S5
// KEYS[1] - 消费中队列
// KEYS[2] - 剩余重试次数结构
// KEYS[3] - 待重试队列
// ARGV[1] - 确认消费成功超时时间
var S5 = redis.NewScript(`
-- 获取[消费中队列]中已经消费超时的数据
local doRetry = function(ids)
	-- 获取剩余重试次数
    local counts = redis.call('HMGET', KEYS[2], unpack(ids))
    for i, v in ipairs(ids) do
        local id = values[i]
        if v ~= nil and v ~= '' and v ~= false and tonumber(v) > 0 then
            -- 剩余重试次数大于 0
			-- 剩余重试次数减 1 并将其添加到[待重试队列]中
            redis.call('HINCRBY', KEYS[2], id, -1)
            redis.call('RPUSH', KEYS[3], id)
        else
            -- 剩余重试次数小于等于 0
            redis.call('HDEL', KEYS[2], id)
        end
    end
end

-- 获取[消费中队列]已经消费超时的数据
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if (#ids > 0) then
	-- 消费超时数据重试处理
    doRetry(ids)
	
    -- 从[消费中队列]中删除已经消费超时的数据
    redis.call('ZREM', KEYS[1], unpack(ids))
end
`)
