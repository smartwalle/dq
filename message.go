package dq

import "time"

// Message 消息结构 (hash)
type Message struct {
	id        string // 消息 id - id
	uuid      string // 消息 uuid - uuid
	queue     string // 队列名称 - qn
	payload   string // 消息内容 - pl
	retry     int    // 剩余重试次数 - rc
	deliverAt int64  // 投递时间
}

type MessageOption func(m *Message)

func WithDeliverAt(deliverAt time.Time) MessageOption {
	return func(m *Message) {
		if !deliverAt.IsZero() {
			m.deliverAt = deliverAt.Unix()
		}
	}
}

func WithDeliverAfter(seconds int64) MessageOption {
	return func(m *Message) {
		m.deliverAt = time.Now().Unix() + seconds
	}
}

func WithPayload(payload string) MessageOption {
	return func(m *Message) {
		m.payload = payload
	}
}

func WithMaxRetry(maxRetry int) MessageOption {
	return func(m *Message) {
		m.retry = maxRetry
	}
}
