package main

import (
	"context"
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

	for i := 0; i < 1000000000; i++ {
		queue.Enqueue(context.Background(), fmt.Sprintf("%d", i), dq.WithDeliverAfter(0), dq.WithMaxRetry(1))
		time.Sleep(time.Millisecond)
	}
}
