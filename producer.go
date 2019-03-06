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
		ropt:        ropt,
		shardsCount: opt.ShardsCount,
	})
	if err != nil {
		return nil, err
	}

	retr := retrier.New(retrier.Config{RetryPolicy: []time.Duration{time.Second * 1}})
	c := make(chan string, opt.PendingBufferSize)

	pr := &Producer{
		c:     c,
		cl:    client,
		notif: opt.ErrorNotifier,
		opt:   opt,
		retr:  retr,
		wg:    &sync.WaitGroup{},
	}

	pr.wg.Add(1)
	go pr.produce()

	return pr, nil
}

// Close queue client.
//
// Function locks until all produced messages will be sent to Redis.
// If PendingBufferSize has huge value - Close can wait long time.
func (p *Producer) Close() {
	close(p.c)
	p.wg.Wait()
}

// Send message.
//
// Message not sended immediately, but pushed to send buffer and sended to Redis
// in other goroutine.
func (p *Producer) Send(m string) {
	p.c <- m
}

func (p *Producer) produce() {
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

		var doSend bool

		if idx >= int(p.opt.PipeBufferSize) {
			doSend = true
		} else if time.Now().Sub(started) >= p.opt.PipePeriod && len(p.c) <= 0 {
			doSend = true
		} else {
			doSend = false
		}

		if !doSend {
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

	p.wg.Done()
}

func (p *Producer) send(shard int, buf []string) {
	if len(buf) == 0 {
		return
	}

	stream := fmt.Sprintf("qu{%d}_%s", shard, p.opt.Name)

	err := p.retr.Do(func() *retrier.Error {
		pipe := p.cl.rDB.Pipeline()

		for _, m := range buf {
			pipe.XAdd(&redis.XAddArgs{
				Stream: stream,
				ID:     "*",
				Values: map[string]interface{}{"m": m},
			})
		}

		_, err := pipe.Exec()
		if err != nil {
			if p.notif != nil {
				p.notif.AmiError(err)
			}

			return retrier.NewError(err, false)
		}

		return nil
	})

	if err != nil && p.notif != nil {
		p.notif.AmiError(err)
	}
}
