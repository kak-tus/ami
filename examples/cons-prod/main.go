package main

import (
	"fmt"
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
			PendingBufferSize: 100000,
		},
		&redis.ClusterOptions{
			Addrs: []string{"172.17.0.1:7001", "172.17.0.1:7002"},
		},
	)
	if err != nil {
		panic(err)
	}

	c := qu.Consume()

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
			qu.Ack(m)
			cons++
		}
		wg2.Done()
	}()

	go func() {
		for {
			if stop {
				break
			}
			qu.Send("{}")
			prod++
		}
		wg1.Done()
	}()

	time.Sleep(time.Second * 1)

	stop = true
	wg1.Wait()

	qu.Close()
	wg2.Wait()
	stopped := time.Now()

	fmt.Printf("Produced %d in %f sec, rps %f\n", prod, stopped.Sub(start).Seconds(), float64(prod)/stopped.Sub(start).Seconds())
	fmt.Printf("Consumed %d in %f sec, rps %f\n", cons, stopped.Sub(start).Seconds(), float64(cons)/stopped.Sub(start).Seconds())
}
