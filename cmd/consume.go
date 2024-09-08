package main

import (
	"context"
	"dq"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func main() {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)
	if _, err := rClient.Ping(context.Background()).Result(); err != nil {
		fmt.Println("redis ping error:", err)
		return
	}

	var q = dq.NewDelayQueue(rClient, "mail")
	var err = q.StartConsume(func(m *dq.Message) error {
		fmt.Println(m.ID(), m.UUID())
		return nil
	})
	if err != nil {
	}

	select {}
}
