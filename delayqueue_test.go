package dq_test

import (
	"context"
	"dq"
	"errors"
	"github.com/redis/go-redis/v9"
	"testing"
)

func Test_GetBlock(t *testing.T) {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)
	if _, err := rClient.Ping(context.TODO()).Result(); err != nil {
		t.Fatal(err)
	}

	//rClient.ZAdd(context.Background(), "k1", redis.Z{Score: 1, Member: "1"})
	//rClient.ZAdd(context.Background(), "k1", redis.Z{Score: float64(time.Now().Unix()), Member: "2"})
	//rClient.ZAdd(context.Background(), "k1", redis.Z{Score: float64(time.Now().Unix() + 1), Member: "3"})
	//rClient.ZAdd(context.Background(), "k1", redis.Z{Score: float64(time.Now().Unix() + 4), Member: "4"})

	raw, err := dq.S4.Run(context.Background(), rClient, []string{"k1"}, "1", 112).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
	t.Log(raw)
}
