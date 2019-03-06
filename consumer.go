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
		ropt:        ropt,
		shardsCount: opt.ShardsCount,
	})
	if err != nil {
		return nil, err
	}

	retr := retrier.New(retrier.Config{RetryPolicy: []time.Duration{time.Second * 1}})
	cCons := make(chan Message, opt.PrefetchCount)
	cAck := make(chan Message, opt.PendingBufferSize)

	cn := &Consumer{
		bufAck:  make(map[string][]Message),
		cAck:    cAck,
		cCons:   cCons,
		cl:      client,
		cntsAck: make(map[string]int),
		notif:   opt.ErrorNotifier,
		opt:     opt,
		retr:    retr,
		wgAck:   &sync.WaitGroup{},
		wgCons:  &sync.WaitGroup{},
	}

	cn.wgAck.Add(1)
	go cn.ack()

	return cn, nil
}

// Start consume from queue.
//
// Start read messages from Redis streams and return channel.
func (c *Consumer) Start() chan Message {
	for i := 0; i < int(c.opt.ShardsCount); i++ {
		c.wgCons.Add(1)
		go c.consume(i)
	}
	return c.cCons
}

// Stop queue client.
//
// Stop reading messages from Redis streams and lock until all
// read messages being processed.
func (c *Consumer) Stop() {
	c.needStop = true

	c.wgCons.Wait()
	close(c.cCons)
	c.stopped = true
}

// Close queue client
//
// Lock until all ACK messages will be sent to Redis.
func (c *Consumer) Close() {
	close(c.cAck)
	c.wgAck.Wait()

	c.retr.Stop()
}

func (c *Consumer) consume(shard int) {
	group := fmt.Sprintf("qu_%s_group", c.opt.Name)
	stream := fmt.Sprintf("qu{%d}_%s", shard, c.opt.Name)

	lastID := "0-0"
	checkBacklog := true

	block := time.Second * 10
	if c.opt.Block > 0 {
		block = c.opt.Block
	}

	for {
		if c.needStop {
			break
		}

		var id string
		if checkBacklog {
			id = lastID
		} else {
			id = ">"
		}

		var res []redis.XStream

		err := c.retr.Do(func() *retrier.Error {
			var err error

			res, err = c.cl.rDB.XReadGroup(&redis.XReadGroupArgs{
				Group:    group,
				Consumer: c.opt.Consumer,
				Streams:  []string{stream, id},
				Count:    c.opt.PrefetchCount,
				Block:    block,
			}).Result()

			if err != nil && err != redis.Nil {
				if c.notif != nil {
					c.notif.AmiError(err)
				}

				return retrier.NewError(err, false)
			}

			return nil
		})

		if err != nil {
			if c.notif != nil {
				c.notif.AmiError(err)
			}

			continue
		}

		if checkBacklog && len(res[0].Messages) == 0 {
			checkBacklog = false
			continue
		}

		for _, s := range res {
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
	}

	c.wgCons.Done()
}

// Ack acknowledges message
//
// Function not only do XACK call, but additionaly it deletes message
// from stream with XDELETE.
// Ack do not do immediately, but pushed to send buffer and sended to Redis
// in other goroutine.
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
		} else if time.Now().Sub(started) >= c.opt.PipePeriod && len(c.cAck) <= 0 {
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

	err := c.retr.Do(func() *retrier.Error {
		pipe := c.cl.rDB.Pipeline()

		for _, m := range c.bufAck[stream][0:c.cntsAck[stream]] {
			pipe.XAck(m.Stream, m.Group, m.ID)

			cmd := redis.NewIntCmd("XDEL", m.Stream, m.ID)

			// Error processed in exec
			_ = pipe.Process(cmd)
		}

		_, err := pipe.Exec()

		if err != nil {
			if c.notif != nil {
				c.notif.AmiError(err)
			}

			return retrier.NewError(err, false)
		}

		return nil
	})

	if err != nil && c.notif != nil {
		c.notif.AmiError(err)
	}

	c.cntsAck[stream] = 0
}
