package main

import (
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

	for i := 0; i < 10000; i++ {
		qu.Send("{}")
	}
	qu.Close()
}
