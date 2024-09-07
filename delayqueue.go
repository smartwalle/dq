package dq

import (
	"fmt"
	"github.com/redis/go-redis/v9"
)

// 消息结构(hash)
// id -- 消息业务id
// uuid -- 消息唯一id
// qn -- 队列名称
// tp -- 消息类型
// pl -- 消息内容
// dt -- 消息投递时间
// rc -- 剩余重试次数

// 延迟队列(sorted set) - member: 消息id，score: 消费时间
// 待处理队列(list) - element: 消息 uuid
// 处理中队列(sorted set) - member: 消息id, score: 确认处理成功超时时间
// 待重试队列(list) - element: 消息 uuid

// S0 添加消息
// KEYS[1] - 延迟队列
// KEYS[2] - 消息结构
//
// ARGV[1] - 消息 id
// ARGV[2] - 消息 uuid
// ARGV[3] - 消费时间
// ARGV[4] - 队列名称
// ARGV[5] - 消息类型
// ARGV[6] - 消息内容
// ARGV[7] - 剩余重试次数
var S0 = redis.NewScript(`
-- 添加到[延迟队列]
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
-- 获取当前时间
local time = redis.call('TIME')
local timestamp = tonumber(time[1])
-- 写入消息结构
redis.call('HMSET', KEYS[2], 'id', ARGV[1], 'uuid', ARGV[2], 'qn', ARGV[4], 'tp', ARGV[5], 'pl', ARGV[6], 'dt', timestamp, 'rc', ARGV[7])
`)

// S1 将消息从[延迟队列]转移到[待处理队列]
// KEYS[1] - 延迟队列
// KEYS[2] - 待处理队列
// KEYS[3] - TaskKeyPrefix
// ARGV[1] - 消费时间
// ARGV[2] - 单次处理数量
var S1 = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if (#ids > 0) then
    for _, id in ipairs(ids) do
        local k1 = KEYS[3]..id
        local uuid = redis.call('HGET', k1, 'uuid')
        local k2 = KEYS[3]..uuid
        redis.call('RPUSH', KEYS[2], uuid)
        redis.call('ZREM', KEYS[1], id)
        redis.call('RENAME', k1, k2)
    end
end
`)

// S2 将消息从[待处理队列]转移到[处理中队列]
// KEYS[1] - 待处理队列
// KEYS[2] - 处理中队列
// ARGV[1] - 确认处理成功超时时间
var S2 = redis.NewScript(`
local uuid = redis.call('LPOP', KEYS[1])
if (not uuid) then
    return ''
end
redis.call('ZADD', KEYS[2], ARGV[1], uuid)
return uuid
`)

// S3
// KEYS[1] - 处理中队列
// KEYS[2] - 待重试队列
// KEYS[3] - TaskKeyPrefix
// ARGV[1] - 确认处理成功超时时间
var S3 = redis.NewScript(`
local doRetry = function(uuid)
    local key = KEYS[3]..uuid

    -- 获取剩余重试次数
    local count = redis.call('HGET', key, 'rc')
    if count ~= nil and count ~= '' and count ~= false and tonumber(count) > 0 then
        -- 剩余重试次数大于 0
        -- 更新剩余重试次数
        redis.call('HINCRBY', key, 'rc', -1)
        -- 添加到[待重试队列]中
        redis.call('RPUSH', KEYS[2], uuid)
    else
        -- 删除[消息结构]
        redis.call('DEL', key)
    end
end

-- 获取[处理中队列]中已经消费超时的数据
local uuids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if (#uuids > 0) then
    for _, uuid in ipairs(uuids) do
        -- 从[处理中队列]中删除已经消费超时的数据
        redis.call('ZREM', KEYS[1], uuid)
        -- 是否需要重试处理
        doRetry(uuid)
    end
end
`)

// S4 将消息从[待重试队列]转移到[处理中队列]
// KEYS[1] - 待重试队列
// KEYS[2] - 处理中队列
// ARGV[1] - 确认处理成功超时时间
var S4 = redis.NewScript(`
local uuid = redis.call('LPOP', KEYS[1])
if (not uuid) then
    return ''
end
redis.call('ZADD', KEYS[2], ARGV[1], uuid)
return uuid
`)

// S5 延长消费成功超时时间
// KEYS[1] - 处理中队列
// ARGV[1] - 消息 uuid
// ARGV[2] - 确认处理成功超时时间
var S5 = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if (not score) then
	return
end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
`)

type DelayQueue struct {
	client redis.UniversalClient
	name   string
}

func QueueKey(qname string) string {
	return fmt.Sprintf("dq:{%s}", qname)
}

func ScheduleKey(qname string) string {
	return fmt.Sprintf("%s:schedule", QueueKey(qname))
}

func PendingKey(qname string) string {
	return fmt.Sprintf("%s:pending", QueueKey(qname))
}

func ActiveKey(qname string) string {
	return fmt.Sprintf("%s:active", QueueKey(qname))
}

func RetryKey(qname string) string {
	return fmt.Sprintf("%s:retry", QueueKey(qname))
}

func TaskKeyPrefix(qname string) string {
	return fmt.Sprintf("%s:task:", QueueKey(qname))
}

func TaskKey(qname, id string) string {
	return fmt.Sprintf("%s%s", TaskKeyPrefix(qname), id)
}

func NewDelayQueue(client redis.UniversalClient, name string) *DelayQueue {
	var q = &DelayQueue{}
	q.client = client
	q.name = name
	return q
}
