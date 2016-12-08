package main

import (
	sj "github.com/bitly/go-simplejson"
)

type SrcProvider interface {
	Init(ctx *sj.Json) error
	GetNextMsg() ([][]byte, error)
	Ack() error
	Destory() error
}

func NewSrcProvider(srcType string) SrcProvider {
	switch srcType {
	case "nsq":
		return new(NsqSrc)
	case "dm":
		return new(DmSrc)
	case "redis":
		return new(RedisSrc)
	default:
		return nil
	}
	return nil
}
