package internal_test

import (
	"context"
	"dq"
	"dq/internal"
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
		time.Now().UnixMilli(),
		queue,
		"message body",
		2,
	}
	raw, err := internal.ScheduleScript.Run(context.Background(), redisClient, keys, args...).Result()
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
		dq.MessageKey(queue, id),
	}
	var args = []interface{}{
		id,
	}
	raw, err := internal.RemoveScript.Run(context.Background(), redisClient, keys, args...).Result()
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
	raw, err := internal.ScheduleToPendingScript.Run(context.Background(), redisClient, keys, args...).Result()
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
	raw, err := internal.PendingToActiveScript.Run(context.Background(), redisClient, keys).Result()
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
	}
	raw, err := internal.ActiveToRetryScript.Run(context.Background(), redisClient, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}

func Test_AckScript(t *testing.T) {
	var queue = "mail"

	var keys = []string{
		dq.ActiveKey(queue),
		dq.MessageKey(queue, "a2d0665c-e73a-41dd-a8e9-ebb25930ff73"),
	}
	raw, err := internal.AckScript.Run(context.Background(), redisClient, keys).Result()
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
		dq.MessageKey(queue, "5748af6e-937d-4f6c-8b52-500892d998ea"),
	}
	raw, err := internal.NackScript.Run(context.Background(), redisClient, keys).Result()
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
	raw, err := internal.RetryToAciveScript.Run(context.Background(), redisClient, keys).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}
