package main

import (
	"errors"
	"time"

	nsq "github.com/bitly/go-nsq"
	sj "github.com/bitly/go-simplejson"
)

type NsqSrc struct {
	consumer   *nsq.Consumer
	config     *nsq.Config
	nsqMsgChan chan nsq.Message
	timeOutSec int
}

func (p *NsqSrc) GetNextMsg() ([][]byte, error) {
	select {
	case nsqMsg := <-p.nsqMsgChan:
		byteArrs := make([][]byte, 1)
		byteArrs[0] = nsqMsg.Body
		return byteArrs, nil
	case <-time.After(time.Duration(p.timeOutSec) * time.Second):

	}
	return [][]byte{}, nil
}

func (p *NsqSrc) Ack() error {
	return nil
}

func (p *NsqSrc) Destory() error {
	p.consumer.Stop()
	return nil
}

func (p *NsqSrc) Init(ctx *sj.Json) error {
	var err error
	p.nsqMsgChan = make(chan nsq.Message) // not buffered

	p.timeOutSec = ctx.Get("timeout_sec").MustInt()
	nsqLookupdAddrs := ctx.Get("nsq_lookupd_addrs").MustStringArray()
	if len(nsqLookupdAddrs) <= 0 {
		return errors.New("no nsq_lookupd_addr in provided")
	}
	topic := ctx.Get("topic").MustString()
	channel := ctx.Get("channel").MustString()

	p.config = nsq.NewConfig()
	p.consumer, err = nsq.NewConsumer(topic, channel, p.config)
	if p.consumer == nil || err != nil {
		return err
	}

	p.consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		p.nsqMsgChan <- *message
		return nil
	}))

	err = p.consumer.ConnectToNSQLookupds(nsqLookupdAddrs) // ip:port
	if err != nil {
		return errors.New("Could not connect to nsqLookupd")
	}

	return nil
}
