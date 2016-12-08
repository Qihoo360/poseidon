package job

import (
	"errors"
	"math/rand"
	"strings"

	sj "github.com/bitly/go-simplejson"
	"github.com/donnie4w/go-logger/logger"
)

type TestProcessor struct {
	id int

	itemChans [](chan Item)
}

func (p *TestProcessor) Init(ctx *sj.Json, id int, itemChans [](chan Item)) error {
	p.id = id
	p.itemChans = itemChans

	return nil
}

func (p *TestProcessor) Process(msg string) error {
	msgArr := strings.Split(msg, "\n")
	if len(msgArr) <= 0 {
		return nil
	}
	for _, v := range msgArr {
		afterTrim := strings.TrimSpace(v)
		if len(afterTrim) <= 0 {
			logger.Debugf("processor %d empty msg: [%s]", p.id, msg)
			continue
		}
		item, err := p.getItem(afterTrim)
		if err != nil || item == nil {
			logger.Errorf("processor %d msg fail: [%s], err: %v", p.id, v, err)
			logger.Errorf("processor %d msg msgArr: [%v], msg: %s", p.id, msgArr, msg)
			continue
		}
		logger.Debugf("processor %d msg ok: [%s]", p.id, v)

		index := p.getCollectorIndex(BKDRHash(item.Content), len(p.itemChans))
		p.itemChans[index] <- *item
		logger.Debugf("processor %d push successful to index: %d", p.id, index)
	}

	return nil
}

func (p *TestProcessor) Tick() error {
	logger.Debug("processor Tick", p.id)
	return nil
}

func (p *TestProcessor) Destory() error {
	logger.Debug("processor destroy", p.id)
	return nil
}

func (p *TestProcessor) getItem(msg string) (*Item, error) {
	if len(msg) <= 0 {
		return nil, errors.New("msg empty")
	}

	item := Item{
		Category: "test_collector",
		Id:       msg,
		Content:  "content: " + msg,
		RawMsg:   "rawMsg: " + msg,
	}
	return &item, nil
}

func (p *TestProcessor) getCollectorIndex(hash int, size int) int {
	return (hash + rand.Intn(10)) % size
}
