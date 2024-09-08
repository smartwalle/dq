package dq_test

import (
	"context"
	"dq"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"os"
	"testing"
	"time"
)

var redisClient redis.UniversalClient

func TestMain(m *testing.M) {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)
	if _, err := rClient.Ping(context.Background()).Result(); err != nil {
		fmt.Println("redis ping error:", err)
		return
	}
	redisClient = rClient
	os.Exit(m.Run())
}

func Test_QueueKey(t *testing.T) {
	t.Log(dq.QueueKey("mail"))
	t.Log(dq.ScheduleKey("mail"))
	t.Log(dq.PendingKey("mail"))
	t.Log(dq.ActiveKey("mail"))
	t.Log(dq.RetryKey("mail"))
	t.Log(dq.MessageKey("mail", "11"))
	t.Log(dq.MessageKey("mail", "22"))
}

func Test_ScheduleScript(t *testing.T) {
	var queue = "mail"
	var id = "t1"

	var keys = []string{
		dq.ScheduleKey(queue),
		dq.MessageKey(queue, id),
	}
	var args = []interface{}{
		id,
		uuid.New().String(),
		time.Now().Unix(),
		queue,
		"message body",
		2,
	}
	raw, err := dq.ScheduleScript.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_RemoveScript(t *testing.T) {
	var queue = "mail"
	var id = "t1"

	var keys = []string{
		dq.ScheduleKey(queue),
		dq.MessageKeyPrefix(queue),
	}
	var args = []interface{}{
		id,
	}
	raw, err := dq.RemoveScript.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_ScheduleToPendingScript(t *testing.T) {
	var queue = "mail"

	var keys = []string{
		dq.ScheduleKey(queue),
		dq.PendingKey(queue),
		dq.MessageKeyPrefix(queue),
	}
	var args = []interface{}{
		10,
	}
	raw, err := dq.ScheduleToPendingScript.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_PendingToActiveScript(t *testing.T) {
	var queue = "mail"

	var keys = []string{
		dq.PendingKey(queue),
		dq.ActiveKey(queue),
	}
	raw, err := dq.PendingToActiveScript.Run(context.Background(), redisClient, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_ActiveToRetryScript(t *testing.T) {
	var queue = "mail"

	var keys = []string{
		dq.ActiveKey(queue),
		dq.RetryKey(queue),
		dq.MessageKeyPrefix(queue),
	}
	raw, err := dq.ActiveToRetryScript.Run(context.Background(), redisClient, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_AckScript(t *testing.T) {
	var queue = "mail"

	var keys = []string{
		dq.ActiveKey(queue),
		dq.MessageKeyPrefix(queue),
	}
	var args = []interface{}{
		"d7d46645-cac9-4ebb-b816-ba0278672ea2",
	}
	raw, err := dq.AckScript.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_NackScript(t *testing.T) {
	var queue = "mail"

	var keys = []string{
		dq.ActiveKey(queue),
		dq.RetryKey(queue),
		dq.MessageKeyPrefix(queue),
	}
	var args = []interface{}{
		"d7d46645-cac9-4ebb-b816-ba0278672ea2",
	}
	raw, err := dq.NackScript.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_RetryToAciveScript(t *testing.T) {
	var queue = "mail"

	var keys = []string{
		dq.RetryKey(queue),
		dq.ActiveKey(queue),
	}
	raw, err := dq.RetryToAciveScript.Run(context.Background(), redisClient, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func TestDelayQueue_Enqueue(t *testing.T) {
	var q = dq.NewDelayQueue(redisClient, "mail")
	for i := 0; i < 1000; i++ {
		var err = q.Enqueue(context.Background(), fmt.Sprintf("%d", time.Now().UnixNano()), dq.WithDeliverAfter(10))
		if err != nil {
			t.Fatal(err)
		}
	}
}
