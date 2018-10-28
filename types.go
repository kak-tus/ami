package ami

import (
	"sync"
	"time"

	"git.aqq.me/go/retrier"
	"github.com/go-redis/redis"
)

// Message from queue
type Message struct {
	Body   string
	ID     string
	Stream string
	Group  string
}

type client struct {
	rDB *redis.ClusterClient
	opt clientOptions
}

type clientOptions struct {
	name        string
	shardsCount int8
	ropt        *redis.ClusterOptions
}

// Producer client for Ami
type Producer struct {
	cl   *client
	wg   *sync.WaitGroup
	opt  ProducerOptions
	retr *retrier.Retrier
	c    chan string
}

// ProducerOptions - options for producer client for Ami
type ProducerOptions struct {
	Name              string
	ShardsCount       int8
	PendingBufferSize int64
	PipeBufferSize    int64
	PipePeriod        time.Duration
}

// Consumer client for Ami
type Consumer struct {
	cl       *client
	wgCons   *sync.WaitGroup
	wgAck    *sync.WaitGroup
	opt      ConsumerOptions
	cCons    chan Message
	cAck     chan Message
	needStop bool
	retr     *retrier.Retrier
	stopped  bool
}

// ConsumerOptions - options for consumer client for Ami
type ConsumerOptions struct {
	Name              string
	Consumer          string
	ShardsCount       int8
	PrefetchCount     int64
	Block             time.Duration
	PendingBufferSize int64
	PipeBufferSize    int64
	PipePeriod        time.Duration
}
