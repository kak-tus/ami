package ami

import (
	"fmt"
	"sync"
	"time"

	"git.aqq.me/go/retrier"
	"github.com/go-redis/redis"
)

// NewProducer creates new producer client for Ami
func NewProducer(opt ProducerOptions, ropt *redis.ClusterOptions) (*Producer, error) {
	client, err := newClient(clientOptions{
		name:        opt.Name,
		shardsCount: opt.ShardsCount,
		ropt:        ropt,
	})
	if err != nil {
		return nil, err
	}

	retr := retrier.New(retrier.Config{RetryPolicy: []time.Duration{time.Second * 1}})
	c := make(chan string, opt.PendingBufferSize)

	pr := &Producer{
		cl:   client,
		wg:   &sync.WaitGroup{},
		opt:  opt,
		c:    c,
		retr: retr,
	}

	pr.wg.Add(1)
	go pr.produce()

	return pr, nil
}

// Close queue client
func (p *Producer) Close() {
	close(p.c)
	p.wg.Wait()
}

// Send message
func (p *Producer) Send(m string) {
	p.c <- m
}

func (p *Producer) produce() {
	defer p.wg.Done()

	shard := 0

	buf := make([]string, p.opt.PipeBufferSize)
	idx := 0
	started := time.Now()

	for {
		m, more := <-p.c

		if !more {
			p.send(shard, buf[0:idx])
			break
		}

		buf[idx] = m
		idx++

		if idx < int(p.opt.PipeBufferSize) && time.Now().Sub(started) < p.opt.PipePeriod {
			continue
		}

		p.send(shard, buf[0:idx])

		idx = 0
		started = time.Now()

		shard++
		if shard >= int(p.opt.ShardsCount) {
			shard = 0
		}
	}
}

func (p *Producer) send(shard int, buf []string) {
	if len(buf) == 0 {
		return
	}

	pipe := p.cl.rDB.Pipeline()
	stream := fmt.Sprintf("qu{%d}_%s", shard, p.opt.Name)

	p.retr.Do(func() *retrier.Error {
		for _, m := range buf {
			pipe.XAdd(&redis.XAddArgs{
				Stream: stream,
				ID:     "*",
				Values: map[string]interface{}{"m": m},
			})
		}

		_, err := pipe.Exec()
		if err != nil {
			return retrier.NewError(err, false)
		}

		return nil
	})
}
