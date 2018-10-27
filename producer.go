package ami

import (
	"fmt"

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

	for {
		m, more := <-q.cProd
		if !more {
			break
		}

		stream := fmt.Sprintf("qu{%d}_%s", shard, q.opt.Name)

		q.retr.Do(func() *retrier.Error {
			err := q.rDB.XAdd(&redis.XAddArgs{
				Stream: stream,
				ID:     "*",
				Values: map[string]interface{}{"m": m},
			}).Err()
			if err != nil {
				return retrier.NewError(err, false)
			}

			return nil
		})

		shard++
		if shard >= int(q.opt.ShardsCount) {
			shard = 0
		}
	}
}
