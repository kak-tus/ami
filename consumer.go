package ami

import (
	"fmt"
	"sync"
	"time"

	"git.aqq.me/go/retrier"
	"github.com/go-redis/redis"
)

// NewConsumer creates new consumer client for Ami
func NewConsumer(opt ConsumerOptions, ropt *redis.ClusterOptions) (*Consumer, error) {
	client, err := newClient(clientOptions{
		name:        opt.Name,
		shardsCount: opt.ShardsCount,
		ropt:        ropt,
	})
	if err != nil {
		return nil, err
	}

	retr := retrier.New(retrier.Config{RetryPolicy: []time.Duration{time.Second * 1}})
	cCons := make(chan Message, opt.PrefetchCount)
	cAck := make(chan Message, opt.PendingBufferSize)

	cn := &Consumer{
		cl:      client,
		wgCons:  &sync.WaitGroup{},
		wgAck:   &sync.WaitGroup{},
		opt:     opt,
		cCons:   cCons,
		cAck:    cAck,
		retr:    retr,
		bufAck:  make(map[string][]Message),
		cntsAck: make(map[string]int),
	}

	cn.wgAck.Add(1)
	go cn.ack()

	return cn, nil
}

// Start consume from queue
func (c *Consumer) Start() chan Message {
	for i := 0; i < int(c.opt.ShardsCount); i++ {
		c.wgCons.Add(1)
		go c.consume(i)
	}
	return c.cCons
}

// Stop queue client
func (c *Consumer) Stop() {
	c.needStop = true

	c.wgCons.Wait()
	close(c.cCons)
	c.stopped = true
}

// Close queue client
func (c *Consumer) Close() {
	close(c.cAck)
	c.wgAck.Wait()

	c.retr.Stop()
}

func (c *Consumer) consume(shard int) {
	defer c.wgCons.Done()

	group := fmt.Sprintf("qu_%s_group", c.opt.Name)
	stream := fmt.Sprintf("qu{%d}_%s", shard, c.opt.Name)

	lastID := "0-0"
	checkBacklog := true

	for {
		c.retr.Do(func() *retrier.Error {
			if c.needStop {
				return nil
			}

			var id string
			if checkBacklog {
				id = lastID
			} else {
				id = ">"
			}

			res := c.cl.rDB.XReadGroup(&redis.XReadGroupArgs{
				Group:    group,
				Consumer: c.opt.Consumer,
				Streams:  []string{stream, id},
				Count:    c.opt.PrefetchCount,
				Block:    c.opt.Block,
			})

			if res.Err() != nil {
				return retrier.NewError(res.Err(), false)
			}

			if checkBacklog && len(res.Val()[0].Messages) == 0 {
				checkBacklog = false
				return nil
			}

			for _, s := range res.Val() {
				for _, m := range s.Messages {
					msg := Message{
						// TODO FIX move to failed
						Body:   m.Values["m"].(string),
						ID:     m.ID,
						Stream: stream,
						Group:  group,
					}

					c.cCons <- msg

					lastID = msg.ID
				}
			}

			return nil
		})

		if c.needStop {
			break
		}
	}
}

// Ack acknowledges message
func (c *Consumer) Ack(m Message) {
	c.cAck <- m
}

func (c *Consumer) ack() {
	defer c.wgAck.Done()

	started := time.Now()

	for {
		m, more := <-c.cAck

		if !more {
			c.sendAckAllStreams()
			break
		}

		if c.bufAck[m.Stream] == nil {
			c.bufAck[m.Stream] = make([]Message, c.opt.PipeBufferSize)
			c.cntsAck[m.Stream] = 0
		}

		c.bufAck[m.Stream][c.cntsAck[m.Stream]] = m
		c.cntsAck[m.Stream]++

		if c.cntsAck[m.Stream] >= int(c.opt.PipeBufferSize) {
			c.sendAckStream(m.Stream)
		}

		if time.Now().Sub(started) >= c.opt.PipePeriod {
			c.sendAckAllStreams()
			started = time.Now()
		}
	}
}

func (c *Consumer) sendAckAllStreams() {
	for stream := range c.bufAck {
		c.sendAckStream(stream)
	}
}

func (c *Consumer) sendAckStream(stream string) {
	if c.cntsAck[stream] <= 0 {
		return
	}

	pipe := c.cl.rDB.Pipeline()

	c.retr.Do(func() *retrier.Error {
		for _, m := range c.bufAck[stream][0:c.cntsAck[stream]] {
			pipe.XAck(m.Stream, m.Group, m.ID)

			cmd := redis.NewIntCmd("XDEL", m.Stream, m.ID)
			pipe.Process(cmd)
		}

		_, err := pipe.Exec()

		if err != nil {
			return retrier.NewError(err, false)
		}

		return nil
	})

	c.cntsAck[stream] = 0
}
