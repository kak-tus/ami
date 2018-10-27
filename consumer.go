package ami

import (
	"fmt"

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
	q.retr.Do(func() *retrier.Error {
		err := q.rDB.XAck(m.Stream, m.Group, m.ID).Err()
		if err != nil {
			return retrier.NewError(err, false)
		}

		return nil
	})

	q.retr.Do(func() *retrier.Error {
		cmd := redis.NewIntCmd("XDEL", m.Stream, m.ID)
		err := q.rDB.Process(cmd)
		if err != nil {
			return retrier.NewError(err, false)
		}

		return nil
	})
}
