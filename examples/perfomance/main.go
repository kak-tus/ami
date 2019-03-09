package main

import (
	"fmt"
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
			PrefetchCount:     1000,
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

	pr, err := ami.NewProducer(
		ami.ProducerOptions{
			ErrorNotifier:     &errorLogger{},
			Name:              "ruthie",
			PendingBufferSize: 10000000,
			PipeBufferSize:    50000,
			PipePeriod:        time.Microsecond * 1000,
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

	start := time.Now()
	prod := 1000000

	for i := 0; i < prod; i++ {
		pr.Send("{}")
	}

	pr.Close()

	stopped := time.Now()

	fmt.Printf(
		"Produced %d in %f sec, rps %f\n",
		prod,
		stopped.Sub(start).Seconds(),
		float64(prod)/stopped.Sub(start).Seconds(),
	)

	c := cn.Start()

	cons := 0

	lock := sync.Mutex{}
	lock.Lock()

	toAck := make([]ami.Message, 0)

	start = time.Now()

	go func() {
		for {
			m, more := <-c
			if !more {
				break
			}

			toAck = append(toAck, m)
			cons++
		}

		lock.Unlock()
	}()

	time.Sleep(time.Second)
	cn.Stop()
	lock.Lock()

	stopped = time.Now()
	fmt.Printf(
		"Consumed %d in %f sec, rps %f\n",
		cons,
		stopped.Sub(start).Seconds(),
		float64(cons)/stopped.Sub(start).Seconds(),
	)

	start = time.Now()

	for _, m := range toAck {
		cn.Ack(m)
	}

	cn.Close()

	stopped = time.Now()

	fmt.Printf(
		"Acked %d in %f sec, rps %f\n",
		cons,
		stopped.Sub(start).Seconds(),
		float64(cons)/stopped.Sub(start).Seconds(),
	)
}

type errorLogger struct{}

func (l *errorLogger) AmiError(err error) {
	println("Got error from Ami:", err.Error())
}
