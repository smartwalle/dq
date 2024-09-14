package internal

import (
	_ "embed"
	"github.com/redis/go-redis/v9"
)

// 消息结构(hash)
// id -- 消息业务id
// uuid -- 消息唯一id
// qn -- 队列名称
// pl -- 消息内容
// dt -- 消息投递时间
// rc -- 剩余重试次数
// c -- 当前消费者id

// 延迟队列(sorted set) - member: 消息id，score: 消费时间
// 待处理队列(list) - element: 消息 uuid
// 处理中队列(sorted set) - member: 消息id, score: 确认处理成功超时时间
// 待重试队列(list) - element: 消息 uuid

//go:embed schedule.lua
var scheduleScript string

// ScheduleScript 添加消息
var ScheduleScript = redis.NewScript(scheduleScript)

//go:embed remove.lua
var removeScript string

// RemoveScript 删除消息
var RemoveScript = redis.NewScript(removeScript)

//go:embed schedule_to_pending.lua
var scheduleToPendingScript string

// ScheduleToPendingScript 将消息从[延迟队列]转移到[待处理队列]
var ScheduleToPendingScript = redis.NewScript(scheduleToPendingScript)

//go:embed pending_to_active.lua
var pendingToActiveScript string

// PendingToActiveScript 将消息从[待处理队列]转移到[处理中队列]
var PendingToActiveScript = redis.NewScript(pendingToActiveScript)

//go:embed active_to_retry.lua
var activeToRetryScript string

// ActiveToRetryScript 将[处理中队列]中已经消费超时的消息转移到[待重试队列]
var ActiveToRetryScript = redis.NewScript(activeToRetryScript)

//go:embed ack.lua
var ackScript string

// AckScript 消费成功
var AckScript = redis.NewScript(ackScript)

//go:embed nack.lua
var nackScript string

// NackScript 消费失败
var NackScript = redis.NewScript(nackScript)

//go:embed retry_to_active.lua
var retryToAciveScript string

// RetryToAciveScript 将消息从[待重试队列]转移到[处理中队列]
var RetryToAciveScript = redis.NewScript(retryToAciveScript)

//go:embed clear_consumer.lua
var clearConsumerScript string

// ClearConsumerScript 清理超时的消费者
var ClearConsumerScript = redis.NewScript(clearConsumerScript)
