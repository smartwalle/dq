package dq_test

import (
	"context"
	"dq"
	"errors"
	"fmt"
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

//func Test_Main(t *testing.T) {
//
//
//	//rClient.ZAdd(context.Background(), "k1", redis.Z{Score: 1, Member: "1"})
//	//rClient.ZAdd(context.Background(), "k1", redis.Z{Score: float64(time.Now().Unix()), Member: "2"})
//	//rClient.ZAdd(context.Background(), "k1", redis.Z{Score: float64(time.Now().Unix() + 1), Member: "3"})
//	//rClient.ZAdd(context.Background(), "k1", redis.Z{Score: float64(time.Now().Unix() + 4), Member: "4"})
//
//	raw, err := dq.S4.Run(context.Background(), rClient, []string{"k1"}, "1", 112).Result()
//	if err != nil && !errors.Is(err, redis.Nil) {
//		t.Fatal(err)
//	}
//	t.Log(raw)
//}

func Test_QueueKey(t *testing.T) {
	t.Log(dq.QueueKey("mail"))
	t.Log(dq.ScheduleKey("mail"))
	t.Log(dq.PendingKey("mail"))
	t.Log(dq.ActiveKey("mail"))
	t.Log(dq.RetryCountKey("mail"))
	t.Log(dq.TaskKey("mail", "11"))
	t.Log(dq.TaskKey("mail", "22"))
}

func Test_S0(t *testing.T) {
	var queue = "mail"
	var id = "t1"

	var keys = []string{
		dq.ScheduleKey(queue),
		dq.TaskKey(queue, id),
		dq.RetryCountKey(queue),
	}
	var args = []interface{}{
		id,
		time.Now().Unix(),
		queue,
		"send",
		"message body",
		10,
	}
	raw, err := dq.S0.Run(context.Background(), redisClient, keys, args...).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}
