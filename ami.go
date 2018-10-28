package ami

import (
	"fmt"

	"github.com/go-redis/redis"
)

func newClient(opt clientOptions) (*client, error) {
	rDB := redis.NewClusterClient(opt.ropt)

	c := &client{
		rDB: rDB,
		opt: opt,
	}

	err := c.init()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *client) init() error {
	group := fmt.Sprintf("qu_%s_group", c.opt.name)
	for i := 0; i < int(c.opt.shardsCount); i++ {
		stream := fmt.Sprintf("qu{%d}_%s", i, c.opt.name)
		err := c.createShard(stream, group)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) createShard(stream string, group string) error {
	xinfo := redis.NewCmd("XINFO", "STREAM", stream)

	err := c.rDB.Process(xinfo)
	if err != nil {
		xgroup := redis.NewCmd("XGROUP", "CREATE", stream, group, "$", "MKSTREAM")
		c.rDB.Process(xgroup)
	}

	// Check after creation
	err = c.rDB.Process(xinfo)
	if err != nil {
		return err
	}

	return nil
}
