package main

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/kak-tus/ami"
)

func main() {
	errLog := &errorLogger{}

	pr, err := ami.NewProducer(
		ami.ProducerOptions{
			ErrorNotifier:     errLog,
			Name:              "ruthie",
			ShardsCount:       10,
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
		pr.Send("{}")
	}

	pr.Close()
}

type errorLogger struct {
}

func (l *errorLogger) AmiError(err error) {
	println("Got error from Ami:", err.Error())
}
