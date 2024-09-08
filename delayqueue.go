package dq

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strings"
	"sync"
	"time"
)

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

func MessageKeyPrefix(qname string) string {
	return fmt.Sprintf("%s:message:", QueueKey(qname))
}

func MessageKey(qname, id string) string {
	return fmt.Sprintf("%s%s", MessageKeyPrefix(qname), id)
}

const (
	kStatusPending   = 0 // 待消费
	kStatusConsuming = 1 // 消费中
)

type Handler func(m *Message) error

type DelayQueue struct {
	client redis.UniversalClient
	name   string
	mu     *sync.Mutex
	status int8
	close  chan struct{}

	defaultTimeout int64
}

func NewDelayQueue(client redis.UniversalClient, name string) *DelayQueue {
	var q = &DelayQueue{}
	q.client = client
	q.name = name
	q.mu = &sync.Mutex{}
	q.status = kStatusPending
	q.close = make(chan struct{}, 1)
	q.defaultTimeout = 10
	return q
}

func (q *DelayQueue) Enqueue(ctx context.Context, id string, opts ...MessageOption) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("必须指定消息 id")
	}
	var m = &Message{}
	m.id = id
	m.uuid = NewUUID()
	m.queue = q.name
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}

	if m.timeout <= 0 {
		m.timeout = q.defaultTimeout
	}

	var keys = []string{
		ScheduleKey(q.name),
		MessageKey(q.name, m.id),
	}
	var args = []interface{}{
		m.id,
		m.uuid,
		m.deliverAt,
		m.queue,
		m.payload,
		m.retry,
		m.timeout,
	}
	if _, err := ScheduleScript.Run(ctx, q.client, keys, args).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) scheduleToPending(ctx context.Context) error {
	var keys = []string{
		ScheduleKey(q.name),
		PendingKey(q.name),
		MessageKeyPrefix(q.name),
	}
	var args = []interface{}{
		1000,
	}
	if _, err := ScheduleToPendingScript.Run(ctx, q.client, keys, args).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) pendingToActiveScript(ctx context.Context) (string, error) {
	var keys = []string{
		PendingKey(q.name),
		ActiveKey(q.name),
		MessageKeyPrefix(q.name),
	}
	raw, err := PendingToActiveScript.Run(ctx, q.client, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}
	uuid, _ := raw.(string)
	return uuid, nil
}

func (q *DelayQueue) activeToRetryScript(ctx context.Context) error {
	var keys = []string{
		ActiveKey(q.name),
		RetryKey(q.name),
		MessageKeyPrefix(q.name),
	}

	_, err := ActiveToRetryScript.Run(ctx, q.client, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) retryToAciveScript(ctx context.Context) (string, error) {
	var keys = []string{
		RetryKey(q.name),
		ActiveKey(q.name),
	}
	raw, err := RetryToAciveScript.Run(ctx, q.client, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}
	uuid, _ := raw.(string)
	return uuid, nil
}

func (q *DelayQueue) ack(ctx context.Context, uuid string) error {
	var keys = []string{
		ActiveKey(q.name),
		MessageKeyPrefix(q.name),
	}
	var args = []interface{}{
		uuid,
	}
	_, err := AckScript.Run(ctx, q.client, keys, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) nack(ctx context.Context, uuid string) error {
	var keys = []string{
		ActiveKey(q.name),
		RetryKey(q.name),
		MessageKeyPrefix(q.name),
	}
	var args = []interface{}{
		uuid,
	}
	_, err := NackScript.Run(ctx, q.client, keys, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) consumeMessage(ctx context.Context, uuid string, handler Handler) error {
	if uuid == "" {
		return nil
	}

	var data, err = q.client.HGetAll(ctx, MessageKey(q.name, uuid)).Result()
	if err != nil {
		return err
	}

	var m = &Message{}
	m.id = data["id"]
	m.uuid = data["uuid"]
	m.queue = data["qn"]
	m.payload = data["pl"]

	err = handler(m)
	if err != nil {
		q.nack(ctx, uuid)
		return err
	}
	q.ack(ctx, uuid)
	return nil
}

func (q *DelayQueue) consume(ctx context.Context, handler Handler) (err error) {
	if err = q.scheduleToPending(ctx); err != nil {
		return err
	}

	var uuid = ""

	// 消费消息
	for {
		uuid, err = q.pendingToActiveScript(ctx)
		if err != nil {
			return err
		}
		if uuid == "" {
			break
		}
		if err = q.consumeMessage(ctx, uuid, handler); err != nil {
			return err
		}
	}

	// 处理消费超时的消息
	if err = q.activeToRetryScript(ctx); err != nil {
		return err
	}

	// 消费重试消息
	for {
		uuid, err = q.retryToAciveScript(ctx)
		if err != nil {
			return err
		}
		if uuid == "" {
			break
		}
		if err = q.consumeMessage(ctx, uuid, handler); err != nil {
			return err
		}
	}
	return nil
}

func (q *DelayQueue) StartConsume(handler Handler) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.status == kStatusConsuming {
		return nil
	}
	q.status = kStatusConsuming

	go func() {
		var ticker = time.NewTicker(time.Second)
	runLoop:
		for {
			select {
			case <-q.close:
				break runLoop
			default:
				select {
				case <-ticker.C:
					rErr := q.consume(context.Background(), handler)
					if rErr != nil {
						// TODO error
					}
				case <-q.close:
					break runLoop
				}
			}
		}
		ticker.Stop()
	}()
	return nil
}

func (q *DelayQueue) StopConsume() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.status != kStatusConsuming {
		return nil
	}
	q.status = kStatusPending
	close(q.close)
	return nil
}
