package ami

import (
	"fmt"
	"time"

	"git.aqq.me/go/retrier"
	"github.com/go-redis/redis"
)

// Send message
func (q *Qu) Send(m string) {
	q.cProd <- m
}

func (q *Qu) produce() {
	q.wgProd.Add(1)
	defer q.wgProd.Done()

	shard := 0

	buf := make([]string, q.opt.PipeBufferSize)
	idx := 0
	started := time.Now()

	for {
		m, more := <-q.cProd

		if !more {
			q.send(shard, buf[0:idx])
			break
		}

		buf[idx] = m
		idx++

		if idx < int(q.opt.PipeBufferSize) && time.Now().Sub(started) < q.opt.PipePeriod {
			continue
		}

		q.send(shard, buf[0:idx])

		idx = 0
		started = time.Now()

		shard++
		if shard >= int(q.opt.ShardsCount) {
			shard = 0
		}
	}
}

func (q *Qu) send(shard int, buf []string) {
	if len(buf) == 0 {
		return
	}

	pipe := q.rDB.Pipeline()
	stream := fmt.Sprintf("qu{%d}_%s", shard, q.opt.Name)

	q.retr.Do(func() *retrier.Error {
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
