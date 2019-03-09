package main

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/kak-tus/ami"
)

func main() {
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

	for i := 0; i < 10000; i++ {
		pr.Send("{}")
	}

	pr.Close()
}

type errorLogger struct{}

func (l *errorLogger) AmiError(err error) {
	println("Got error from Ami:", err.Error())
}
