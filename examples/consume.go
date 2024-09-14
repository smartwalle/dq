package main

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/smartwalle/dq"
	"os"
	"os/signal"
	"syscall"
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
		fmt.Println(time.Now().UnixMilli(), "1111", m.ID(), m.UUID())
		time.Sleep(time.Second * 31)
		fmt.Println(time.Now().UnixMilli(), "2222", m.ID(), m.UUID())
		return false
	})
	if err != nil {
		fmt.Println("Consume Error", err)
		return
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sig:
	}
	fmt.Println("Close", queue.StopConsume())
}
