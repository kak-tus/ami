package ami

import (
	"fmt"
	"sync"
	"time"

	"git.aqq.me/go/retrier"
	"github.com/go-redis/redis"
)

// NewQu creates new queue client for ami
func NewQu(opt Options, ropt *redis.ClusterOptions) (*Qu, error) {
	rDB := redis.NewClusterClient(ropt)

	cCons := make(chan Message, opt.PrefetchCount)
	r := retrier.New(retrier.Config{RetryPolicy: []time.Duration{time.Second * 1}})
	cProd := make(chan string, opt.PendingBufferSize)
	cAck := make(chan Message, opt.PendingBufferSize)

	q := &Qu{
		rDB:    rDB,
		wgCons: &sync.WaitGroup{},
		wgProd: &sync.WaitGroup{},
		wgAck:  &sync.WaitGroup{},
		opt:    opt,
		cCons:  cCons,
		retr:   r,
		cProd:  cProd,
		cAck:   cAck,
	}

	err := q.init()
	if err != nil {
		return nil, err
	}

	go q.produce()
	go q.ack()

	return q, nil
}

// CloseConsumer queue client
func (q *Qu) CloseConsumer() {
	q.needClose = true

	q.wgCons.Wait()
	close(q.cCons)
	q.closedCons = true
}

// CloseProducer queue client
func (q *Qu) CloseProducer() {
	close(q.cProd)
	q.wgProd.Wait()
	q.closedProd = true
}

// Close queue client
func (q *Qu) Close() {
	if !q.closedCons {
		q.CloseConsumer()
	}

	if !q.closedProd {
		q.CloseProducer()
	}

	close(q.cAck)
	q.wgAck.Wait()

	q.retr.Stop()
}

func (q *Qu) init() error {
	group := fmt.Sprintf("qu_%s_group", q.opt.Name)
	for i := 0; i < int(q.opt.ShardsCount); i++ {
		stream := fmt.Sprintf("qu{%d}_%s", i, q.opt.Name)
		err := q.createShard(stream, group)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *Qu) createShard(stream string, group string) error {
	xinfo := redis.NewCmd("XINFO", "STREAM", stream)

	err := q.rDB.Process(xinfo)
	if err != nil {
		xgroup := redis.NewCmd("XGROUP", "CREATE", stream, group, "$", "MKSTREAM")
		q.rDB.Process(xgroup)
	}

	// Check after creation
	err = q.rDB.Process(xinfo)
	if err != nil {
		return err
	}

	return nil
}
