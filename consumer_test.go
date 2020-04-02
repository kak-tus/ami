package ami

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"
)

func TestNewConsumer(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	ntf := newNotifier(t)

	consOpt := ConsumerOptions{
		Block:          time.Second,
		ErrorNotifier:  ntf,
		PipeBufferSize: 2,
	}

	prOpt := ProducerOptions{
		ErrorNotifier:  ntf,
		PipeBufferSize: 1,
	}

	rdOpt := &redis.ClusterOptions{Addrs: []string{s.Addr()}}

	c, err := NewConsumer(consOpt, &redis.ClusterOptions{})
	assert.Error(t, err, "must not be an error")

	c, err = NewConsumer(consOpt, rdOpt)
	assert.NoError(t, err, "must not be an error")

	p, err := NewProducer(prOpt, rdOpt)
	assert.NoError(t, err, "must not be an error")

	ch := c.Start()

	p.Send("ok")
	p.Send("ok")
	p.Send("ok")

	p.Close()

	select {
	case msg := <-ch:
		assert.Equal(t, "ok", msg.Body, "Got unexpected message")
		c.Ack(msg)
	case <-ntf.IsErr:
		assert.FailNow(t, "got an error")
	case <-time.After(time.Second):
		assert.FailNow(t, "must not wait for a long time")
	}

	closed := make(chan bool)

	go func() {
		c.Stop()
		c.Close()
		closed <- true
	}()

	select {
	case <-closed:
	case <-ntf.IsErr:
		assert.FailNow(t, "got an error")
	case <-time.After(time.Second):
		assert.FailNow(t, "don't closed in time")
	}
}

type notifier struct {
	t     *testing.T
	IsErr chan bool
}

func newNotifier(t *testing.T) *notifier {
	return &notifier{t: t, IsErr: make(chan bool)}
}

func (n *notifier) AmiError(err error) {
	n.IsErr <- true
	assert.FailNow(n.t, err.Error(), "must not got an error from interface")
}
