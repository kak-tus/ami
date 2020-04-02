package ami

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"
)

func TestNewProducer(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	rdOpt := &redis.ClusterOptions{Addrs: []string{s.Addr()}}

	p, err := NewProducer(ProducerOptions{}, rdOpt)
	assert.NoError(t, err, "must not be an error")

	p.Send("ok")

	p.Close()
}
