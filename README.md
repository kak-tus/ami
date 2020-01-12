# Ami

Go client to reliable queues based on [Redis Cluster Streams](https://redis.io/topics/streams-intro).

[![Godoc](https://godoc.org/github.com/kak-tus/ami?status.svg)](http://godoc.org/github.com/kak-tus/ami)
[![Coverage](http://gocover.io/_badge/github.com/kak-tus/ami)](https://gocover.io/github.com/kak-tus/ami)
[![Go Report Card](https://goreportcard.com/badge/github.com/kak-tus/ami)](https://goreportcard.com/report/github.com/kak-tus/ami)

## Consume/produce performance

Performance is dependent from:
- Redis Cluster nodes count;
- ping RTT from client to Redis Cluster master nodes;
- network speed between nodes;
- message sizes;
- Ami configuration.

As example, 10-nodes Redis Cluster with half of nodes in other datacenter (50 msec ping), 1 master/1 slave, with message "{}" got:
```
$ go run examples/performance/main.go
Produced 1000000 in 3.423883 sec, rps 292066.022156
Consumed 151000 in 1.049238 sec, rps 143913.931722
Acked 151000 in 0.973587 sec, rps 155096.612263
```

## Producer example

```
	type errorLogger struct{}

	func (l *errorLogger) AmiError(err error) {
		println("Got error from Ami:", err.Error())
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

	for i := 0; i < 10000; i++ {
		pr.Send("{}")
	}

	pr.Close()
```

## Consumer example

```
	type errorLogger struct{}

	func (l *errorLogger) AmiError(err error) {
		println("Got error from Ami:", err.Error())
	}

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
```
