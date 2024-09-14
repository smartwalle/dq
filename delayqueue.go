package dq

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dq/internal"
	"strings"
	"sync"
	"time"
)

type Handler func(m *Message) bool

type Option func(q *DelayQueue)

func WithFetchLimit(limit int) Option {
	return func(q *DelayQueue) {
		if limit < 0 {
			limit = 1
		}
		q.fetchLimit = limit
	}
}

func WithFetchInterval(d time.Duration) Option {
	return func(q *DelayQueue) {
		if d <= 0 {
			d = time.Second
		}
		q.fetchInterval = d
	}
}

type DelayQueue struct {
	client    redis.UniversalClient
	name      string
	uuid      string
	mu        *sync.Mutex
	consuming bool
	close     chan struct{}

	fetchLimit    int           // 单次最大消费量限制
	fetchInterval time.Duration // 消费间隔时间
}

var (
	ErrInvalidQUeueName   = errors.New("invalid queue name")
	ErrInvalidRedisClient = errors.New("invalid redis client")
	ErrInvalidMessageId   = errors.New("invalid message id")
	ErrConsumerExists     = errors.New("consumer exists")
)

func NewDelayQueue(client redis.UniversalClient, name string, opts ...Option) (*DelayQueue, error) {
	if client == nil {
		return nil, ErrInvalidRedisClient
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return nil, ErrInvalidQUeueName
	}

	var q = &DelayQueue{}
	q.client = client
	q.name = name
	q.uuid = NewUUID()
	q.mu = &sync.Mutex{}
	q.consuming = false
	q.fetchLimit = 1000
	q.fetchInterval = time.Second
	for _, opt := range opts {
		if opt != nil {
			opt(q)
		}
	}
	return q, nil
}

func (q *DelayQueue) UUID() string {
	return q.uuid
}

func (q *DelayQueue) Enqueue(ctx context.Context, id string, opts ...MessageOption) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrInvalidMessageId
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

	var keys = []string{
		internal.ScheduleKey(q.name),
		internal.MessageKey(q.name, m.id),
	}
	var args = []interface{}{
		m.id,
		m.uuid,
		m.deliverAt,
		m.queue,
		m.payload,
		m.retry,
	}
	if _, err := internal.ScheduleScript.Run(ctx, q.client, keys, args).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) Remove(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrInvalidMessageId
	}

	var keys = []string{
		internal.ScheduleKey(q.name),
		internal.MessageKey(q.name, id),
	}
	var args = []interface{}{
		id,
	}
	if _, err := internal.RemoveScript.Run(ctx, q.client, keys, args).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) scheduleToPending(ctx context.Context) error {
	var keys = []string{
		internal.ScheduleKey(q.name),
		internal.PendingKey(q.name),
		internal.MessageKeyPrefix(q.name),
	}
	var args = []interface{}{
		q.fetchLimit,
	}
	if _, err := internal.ScheduleToPendingScript.Run(ctx, q.client, keys, args).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) pendingToActiveScript(ctx context.Context) (string, error) {
	var keys = []string{
		internal.PendingKey(q.name),
		internal.ActiveKey(q.name),
		internal.ConsumerKey(q.name),
	}
	var args = []interface{}{
		q.uuid,
	}
	raw, err := internal.PendingToActiveScript.Run(ctx, q.client, keys, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}
	uuid, _ := raw.(string)
	return uuid, nil
}

func (q *DelayQueue) activeToRetryScript(ctx context.Context) error {
	var keys = []string{
		internal.ActiveKey(q.name),
		internal.RetryKey(q.name),
		internal.ConsumerKey(q.name),
	}

	_, err := internal.ActiveToRetryScript.Run(ctx, q.client, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) retryToAciveScript(ctx context.Context) (string, error) {
	var keys = []string{
		internal.RetryKey(q.name),
		internal.ActiveKey(q.name),
		internal.ConsumerKey(q.name),
	}
	var args = []interface{}{
		q.uuid,
	}
	raw, err := internal.RetryToAciveScript.Run(ctx, q.client, keys, args).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}
	uuid, _ := raw.(string)
	return uuid, nil
}

func (q *DelayQueue) ack(ctx context.Context, uuid string) error {
	var keys = []string{
		internal.ActiveKey(q.name),
		internal.MessageKey(q.name, uuid),
	}
	_, err := internal.AckScript.Run(ctx, q.client, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) nack(ctx context.Context, uuid string) error {
	var keys = []string{
		internal.ActiveKey(q.name),
		internal.RetryKey(q.name),
		internal.MessageKey(q.name, uuid),
	}
	_, err := internal.NackScript.Run(ctx, q.client, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	return nil
}

func (q *DelayQueue) consumeMessage(ctx context.Context, uuid string, handler Handler) error {
	if uuid == "" {
		return nil
	}

	var data, err = q.client.HMGet(ctx, internal.MessageKey(q.name, uuid), "id", "uuid", "qn", "pl").Result()
	if err != nil {
		return err
	}

	var m = &Message{}
	m.id, _ = data[0].(string)
	m.uuid, _ = data[1].(string)
	m.queue, _ = data[2].(string)
	m.payload, _ = data[3].(string)

	if ok := handler(m); ok {
		return q.ack(ctx, uuid)
	}
	return q.nack(ctx, uuid)
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
	if q.consuming {
		return ErrConsumerExists
	}
	q.consuming = true
	q.close = make(chan struct{}, 1)

	// 上报消息者
	value, err := q.client.ZAddNX(context.Background(), internal.ConsumerKey(q.name), redis.Z{Member: q.uuid, Score: float64(time.Now().UnixMilli() + 30*1000)}).Result()
	if err != nil {
		return err
	}
	if value != 1 {
		return ErrConsumerExists
	}

	go func() {
		var ticker = time.NewTicker(time.Second * 10)
	runLoop:
		for {
			select {
			case <-q.close:
				break runLoop
			default:
				select {
				case <-ticker.C:
					// 上报消费者存活状态
					_, rErr := q.client.ZAddXX(context.Background(), internal.ConsumerKey(q.name), redis.Z{Member: q.uuid, Score: float64(time.Now().UnixMilli() + 30*1000)}).Result()
					if rErr != nil {
						q.StopConsume()
					}
				case <-q.close:
					break runLoop
				}
			}
		}
		ticker.Stop()
	}()

	go func() {
		var ticker = time.NewTicker(q.fetchInterval)
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
	if !q.consuming {
		return nil
	}
	q.client.ZRem(context.Background(), internal.ConsumerKey(q.name), q.uuid)
	q.consuming = false
	close(q.close)
	return nil
}
