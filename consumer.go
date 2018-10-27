package ami

import (
	"fmt"
	"time"

	"git.aqq.me/go/retrier"
	"github.com/go-redis/redis"
)

// Consume from queue client
func (q *Qu) Consume() chan Message {
	for i := 0; i < int(q.opt.ShardsCount); i++ {
		go q.consume(i)
	}
	return q.cCons
}

func (q *Qu) consume(shard int) {
	q.wgCons.Add(1)
	defer q.wgCons.Done()

	group := fmt.Sprintf("qu_%s_group", q.opt.Name)
	stream := fmt.Sprintf("qu{%d}_%s", shard, q.opt.Name)

	lastID := "0-0"
	checkBacklog := true

	for {
		q.retr.Do(func() *retrier.Error {
			if q.needClose {
				return nil
			}

			var id string
			if checkBacklog {
				id = lastID
			} else {
				id = ">"
			}

			res := q.rDB.XReadGroup(&redis.XReadGroupArgs{
				Group:    group,
				Consumer: q.opt.Consumer,
				Streams:  []string{stream, id},
				Count:    q.opt.PrefetchCount,
				Block:    q.opt.Block,
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

					q.cCons <- msg

					lastID = msg.ID
				}
			}

			return nil
		})

		if q.needClose {
			break
		}
	}
}

// Ack acknowledges message
func (q *Qu) Ack(m Message) {
	q.cAck <- m
}

func (q *Qu) ack() {
	q.wgAck.Add(1)
	defer q.wgAck.Done()

	buf := make([]Message, q.opt.PipeBufferSize)
	idx := 0
	started := time.Now()

	for {
		m, more := <-q.cAck

		if !more {
			q.sendAck(buf[0:idx])
			break
		}

		buf[idx] = m
		idx++

		if idx < int(q.opt.PipeBufferSize) && time.Now().Sub(started) < q.opt.PipePeriod {
			continue
		}

		q.sendAck(buf[0:idx])

		idx = 0
		started = time.Now()
	}
}

func (q *Qu) sendAck(buf []Message) {
	if len(buf) == 0 {
		return
	}

	pipe := q.rDB.Pipeline()

	q.retr.Do(func() *retrier.Error {
		for _, m := range buf {
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
}
