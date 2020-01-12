package ami

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/imdario/mergo"
	"github.com/ssgreg/repeat"
)

// NewProducer creates new producer client for Ami
//
// Note, that you MUST set in ClusterOptions ReadTimeout and WriteTimeout to at
// least 30-60 seconds, if you set big PipeBufferSize (50000 in examples) and
// if you do producing big messages.
// Reason: one full buffer is send in one big-sized query to Redis.
// So this big query may don't be completed in time and will be retransmitted,
// may be forewer retransmitted.
// This is especially strictly for Producer, because it send big queries with
// big messages. And not so strictly for Consumer, because it send big queries
// with not so big messages (only ids of ACKed messages).
func NewProducer(opt ProducerOptions, ropt *redis.ClusterOptions) (*Producer, error) {
	if err := mergo.Merge(&opt, defaultProducerOptions); err != nil {
		return nil, err
	}

	client, err := newClient(clientOptions{
		name:        opt.Name,
		ropt:        ropt,
		shardsCount: opt.ShardsCount,
	})
	if err != nil {
		return nil, err
	}

	c := make(chan string, opt.PendingBufferSize)

	pr := &Producer{
		c:     c,
		cl:    client,
		notif: opt.ErrorNotifier,
		opt:   opt,
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
	tick := time.NewTicker(p.opt.PipePeriod)

	for {
		var doStop bool

		select {
		case m, more := <-p.c:
			if !more {
				doStop = true
			} else {
				buf[idx] = m
				idx++
			}
		case <-tick.C:
		}

		if doStop {
			p.sendWithLock(shard, buf[0:idx])
			break
		}

		var doSend bool

		if idx == int(p.opt.PipeBufferSize) {
			doSend = true
		} else if time.Since(started) >= p.opt.PipePeriod && len(p.c) <= 0 {
			// Don't send by time if there are more messages in channel
			// Prefer to collect them in batch to speedup producing
			doSend = true
		} else {
			doSend = false
		}

		if !doSend {
			continue
		}

		p.sendWithLock(shard, buf[0:idx])

		idx = 0
		started = time.Now()

		shard++
		if shard == int(p.opt.ShardsCount) {
			shard = 0
		}
	}

	p.wg.Done()
}

func (p *Producer) sendWithLock(shard int, buf []string) {
	if len(buf) == 0 {
		return
	}

	args := make([]redis.XAddArgs, len(buf))

	stream := fmt.Sprintf("qu{%d}_%s", shard, p.opt.Name)

	for i, m := range buf {
		args[i] = redis.XAddArgs{
			ID:     "*",
			Stream: stream,
			Values: map[string]interface{}{"m": m},
		}
	}

	p.wg.Add(1)
	go func() {
		p.send(args)
		p.wg.Done()
	}()
}

func (p *Producer) send(args []redis.XAddArgs) {
	err := repeat.Repeat(
		repeat.Fn(func() error {
			pipe := p.cl.rDB.TxPipeline()

			for _, m := range args {
				pipe.XAdd(&m)
			}

			_, err := pipe.Exec()
			if err != nil {
				if p.notif != nil {
					p.notif.AmiError(err)
				}

				return repeat.HintTemporary(err)
			}

			return nil
		}),
		repeat.StopOnSuccess(),
		repeat.WithDelay(repeat.FullJitterBackoff(500*time.Millisecond).Set()),
	)

	if err != nil && p.notif != nil {
		p.notif.AmiError(err)
	}
}
