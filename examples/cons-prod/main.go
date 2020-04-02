package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"
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

	c := cn.Start()

	cons := 0
	prod := 0

	wg1 := sync.WaitGroup{}
	wg1.Add(1)
	wg2 := sync.WaitGroup{}
	wg2.Add(1)

	stop := false

	start := time.Now()

	go func() {
		for {
			m, more := <-c
			if !more {
				break
			}
			cn.Ack(m)
			cons++
		}
		wg2.Done()
	}()

	go func() {
		for {
			if stop {
				break
			}
			pr.Send("{}")
			prod++
		}
		wg1.Done()
	}()

	time.Sleep(time.Second * 1)

	stop = true
	wg1.Wait()
	pr.Close()

	cn.Stop()
	wg2.Wait()
	cn.Close()

	stopped := time.Now()

	fmt.Printf("Produced %d in %f sec, rps %f\n", prod, stopped.Sub(start).Seconds(), float64(prod)/stopped.Sub(start).Seconds())
	fmt.Printf("Consumed %d in %f sec, rps %f\n", cons, stopped.Sub(start).Seconds(), float64(cons)/stopped.Sub(start).Seconds())
}

type errorLogger struct{}

func (l *errorLogger) AmiError(err error) {
	println("Got error from Ami:", err.Error())
}
