// Package job includes mainly 2 interface: Processor & Collector.
//     Processor is supposed to receive a single string (refered to as a "msg")
//     as input, then it outputs one or more semi-finished results(refered to
//     as a "item") via channels to Collector.
//     Collector receive the items and flush them out when appropriate or do
//     anything you like.
package job

import (
	sj "github.com/bitly/go-simplejson"
)

type Item struct {
	Category string
	Id       string
	Content  string
	RawMsg   string
}

type Processor interface {
	Init(ctx *sj.Json, id int, itemChans [](chan Item)) error
	Process(msg string) error
	Tick() error
	Destory() error
}

type Collector interface {
	Init(ctx *sj.Json, id int) error
	Collect(item Item) error
	Tick() error
	Destory() error
}

func NewProcessor(name string) Processor {
	switch name {
	case "LogtoHdfsProcessor":
		return new(LogtoHdfsProcessor)
	case "TestProcessor":
		return new(TestProcessor)
	default:
		return nil
	}
	return nil
}

func NewCollector(name string) Collector {
	switch name {
	case "LogtoHdfsCollector":
		return new(LogtoHdfsCollector)
	case "TestCollector":
		return new(TestCollector)
	default:
		return nil
	}
	return nil
}
