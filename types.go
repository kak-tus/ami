package ami

import (
	"sync"
	"time"

	"git.aqq.me/go/retrier"
	"github.com/go-redis/redis"
)

// Message from queue
type Message struct {
	Body   string // Message content, you interested in
	ID     string // ID of message in Redis stream
	Stream string // Redis stream name
	Group  string // Redis stream group name
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
//
// Optimal values for me is:
/*
	ShardsCount:       10,
	PendingBufferSize: 10000000,
	PipeBufferSize:    50000,
	PipePeriod:        time.Microsecond * 1000,
*/
type ProducerOptions struct {
	// Queue name
	Name string

	// Shard queue along different Redis Cluster nodes
	//
	// Ami queues spreads along cluster by default Redis Cluster ability - shards. Every queue has setuped number of streams with same name, but with different shard number. So different streams are placed at different Redis Cluster nodes.
	// So bigger value get better spreading of queue along cluster. But huge value is not better idea - it got bigger memory usage. Normal value for cluster with 5 masters and 5 slaves - from 5 to 10.
	// May be later will be added auto-sharding option to place queue on each Redis Cluster node.
	// Shards count must have identical values in all producers and consumers of this queue.
	ShardsCount int8

	// Limits maximum amount of ACK messages queue.
	// Bigger value got better ACK perfomance and bigger memory usage.
	// If you your process dies with big amount of ACKed messages, but not already sended to Redis - ACKs will be lost and messages will be processed again.
	PendingBufferSize int64

	// Request to Redis sended in pipe mode with setuped numbers of requests in one batch.
	// Bigger value get better perfomance.
	PipeBufferSize int64

	// If there is no full batch collected - pipe will be sended every setuped period.
	PipePeriod time.Duration
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
	bufAck   map[string][]Message
	cntsAck  map[string]int
}

// ConsumerOptions - options for consumer client for Ami.
//
// Optimal values for me is:
/*
	ShardsCount:       10,
	PrefetchCount:     100,
	PendingBufferSize: 10000000,
	PipeBufferSize:    50000,
	PipePeriod:        time.Microsecond * 1000,
*/
type ConsumerOptions struct {
	// Queue name
	Name string

	// Unique consumer name per queue in Redis Cluster.
	//
	// Pay attention, that if consumer got some messages, and not fully processed
	// and ACKed them, then gone away and not started again - this messages will
	// be in stream forever.
	// If new consumer starts with same name - it got unprocessed messages.
	//
	// Also, if you start two consumers with same name at the same time - they will got same
	// messages, processes them, and only one can ACK message, and second will
	// retry ACKing of this message forever.
	//
	// Now there is no mechanism in Ami, than can move messages from one consumer
	// to other and I am thinking, how to do it.
	Consumer string

	// Shard queue along different Redis Cluster nodes.
	//
	// Ami queues spreads along cluster by default Redis Cluster ability - shards. Every queue has setuped number of streams with same name, but with different shard number. So different streams are placed at different Redis Cluster nodes.
	// So bigger value get better spreading of queue along cluster. But huge value is not better idea - it got bigger memory usage. Normal value for cluster with 5 masters and 5 slaves - from 5 to 10.
	// May be later will be added auto-sharding option to place queue on each Redis Cluster node.
	// Shards count must have identical values in all producers and consumers of this queue.
	ShardsCount int8

	// Maximum amount of messages that can be read from queue at same time.
	// But this is not real amount of messages, that will only be got from Redis.
	// Ami preloads some amount of messages from all shards.
	// So, this value:
	// - limits consumer channel length;
	// - limits maximum amount of messages, that will be read from one shard at one read operation.
	// Bigger PrefetchCount got bigger memory usage, but can get better perfomance.
	PrefetchCount int64

	// BLOCK option of XREADGROUP Redis command
	// https://redis.io/topics/streams-intro
	// Set it to other then 0 value only if you know what you do
	//
	// If you set this value greater then 0, Ami will use this value to block XREADGROUP for this period.
	// Otherwise Ami will use default value - 10 seconds.
	Block time.Duration

	// Limits maximum amount of ACK messages queue.
	// Bigger value got better ACK perfomance and bigger memory usage.
	// If you your process dies with big amount of ACKed messages, but not already sended to Redis - ACKs will be lost and messages will be processed again.
	PendingBufferSize int64

	// Request to Redis sended in pipe mode with setuped numbers of requests in one batch.
	// Bigger value get better perfomance.
	PipeBufferSize int64

	// If there is no full batch collected - pipe will be sended every setuped period.
	PipePeriod time.Duration
}
