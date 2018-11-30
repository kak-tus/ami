# Ami

Go client to reliable queues based on [Redis Cluster Streams](https://redis.io/topics/streams-intro).

[Ami docs](https://godoc.org/github.com/kak-tus/ami).

## Consume/produce perfomance

Perfomance is dependent from:
- Redis Cluster nodes count;
- ping RTT from client to Redis Cluster master nodes;
- Ami configuration.

As example, 10-nodes Redis Cluster with half of nodes in other datacenter (50 msec ping) got near 80000 rps produced from one client. Also, consume/produce rps is not maximally optimal now and can be a little bit better.

## Producer example

```
	pr, err := ami.NewProducer(
		ami.ProducerOptions{
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
```

## Consumer example

```
	cn, err := ami.NewConsumer(
		ami.ConsumerOptions{
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
