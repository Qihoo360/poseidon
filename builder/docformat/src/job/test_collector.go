package job

import (
	sj "github.com/bitly/go-simplejson"
	"github.com/donnie4w/go-logger/logger"
	"math/rand"
	"time"
)

type TestCollector struct {
	id int
}

func (p *TestCollector) Init(ctx *sj.Json, id int) error {
	p.id = id
	return nil
}

func (p *TestCollector) Collect(item Item) error {
	logger.Debugf("collector %d get item: %v", p.id, item)
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
	if p.id == 1 || p.id == 2 {
		time.Sleep(time.Millisecond * 20000)
	}

	return nil
}

func (p *TestCollector) Destory() error {
	logger.Debug("collector destroy", p.id)
	return nil
}

func (p *TestCollector) Tick() error {
	logger.Debug("collector tick", p.id)
	return nil
}
