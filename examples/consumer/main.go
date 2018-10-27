package main

import (
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/kak-tus/ami"
)

func main() {
	qu, err := ami.NewQu(
		ami.Options{
			Name:              "ruthie",
			Consumer:          "alice",
			ShardsCount:       10,
			PrefetchCount:     100,
			Block:             time.Second,
			PendingBufferSize: 10000000,
			PipeBufferSize:    50000,
			PipePeriod:        time.Microsecond * 1000,
		},
		&redis.ClusterOptions{
			Addrs: []string{"172.17.0.1:7001", "172.17.0.1:7002"},
		},
	)
	if err != nil {
		panic(err)
	}

	c := qu.Consume()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for {
			m, more := <-c
			if !more {
				break
			}
			println("Got", m.Body, "ID", m.ID)
			qu.Ack(m)
		}
		wg.Done()
	}()

	time.Sleep(time.Second)

	qu.CloseConsumer()
	wg.Wait()

	qu.Close()
}
