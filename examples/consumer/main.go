package main

import (
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/kak-tus/ami"
)

func main() {
	cn, err := ami.NewConsumer(
		ami.ConsumerOptions{
			Consumer:          "alice",
			ErrorNotifier:     &errorLogger{},
			Name:              "ruthie",
			PendingBufferSize: 10000000,
			PipeBufferSize:    50000,
			PipePeriod:        time.Microsecond * 1000,
			PrefetchCount:     100,
			ShardsCount:       10,
		},
		&redis.ClusterOptions{
			Addrs:        []string{"172.17.0.1:7001", "172.17.0.1:7002"},
			ReadTimeout:  time.Second * 60,
			WriteTimeout: time.Second * 60,
		},
	)
	if err != nil {
		panic(err)
	}

	c := cn.Start()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for {
			m, more := <-c
			if !more {
				break
			}
			println("Got", m.Body, "ID", m.ID)
			cn.Ack(m)
		}
		wg.Done()
	}()

	time.Sleep(time.Second)

	cn.Stop()
	wg.Wait()

	cn.Close()
}

type errorLogger struct{}

func (l *errorLogger) AmiError(err error) {
	println("Got error from Ami:", err.Error())
}
