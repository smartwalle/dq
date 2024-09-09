package main

import (
	"dq"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

func main() {
	var opt = &redis.Options{}
	opt.Addr = "127.0.0.1:6379"
	opt.Password = ""
	opt.DB = 1

	var rClient = redis.NewClient(opt)

	var queue, err = dq.NewDelayQueue(rClient, "mail")
	if err != nil {
		fmt.Println("NewDelayQueue Error", err)
		return
	}
	err = queue.StartConsume(func(m *dq.Message) bool {
		fmt.Println(time.Now().UnixMilli(), m.ID(), m.UUID())
		return false
	})
	if err != nil {
		fmt.Println("Consume Error", err)
		return
	}

	select {}
}
