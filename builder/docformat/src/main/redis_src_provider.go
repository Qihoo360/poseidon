package main

import (
	"common"
	"errors"
	"strings"

	sj "github.com/bitly/go-simplejson"
	"github.com/donnie4w/go-logger/logger"
	"github.com/garyburd/redigo/redis"
)

type RedisSrc struct {
	pool  *redis.Pool
	topic string
}

func (p *RedisSrc) GetNextMsg() ([][]byte, error) {
	conn := p.pool.Get()
	defer conn.Close()
	s, err := redis.String(conn.Do("RPOP", p.topic))
	byteArr := make([][]byte, 1)
	byteArr[0] = []byte(s)
	return byteArr, err
}

func (p *RedisSrc) Ack() error {
	return nil
}

func (p *RedisSrc) Destory() error {
	return nil
}

func (p *RedisSrc) Init(ctx *sj.Json) error {
	redisConf := ctx.Get("redis_config").MustString()
	if redisConf == "" {
		return errors.New("redis config empty")
	}
	redisConfArr := strings.Split(redisConf, ":")
	if len(redisConfArr) < 3 {
		return errors.New("redis conf invalid")
	}

	redisHost := redisConfArr[0]
	redisPort := redisConfArr[1]
	redisAuth := redisConfArr[2]
	p.pool = common.NewPool(redisHost+":"+redisPort, redisAuth, 3, 240)
	conn := p.pool.Get()
	defer conn.Close()
	response, err := redis.String(conn.Do("PING"))
	if err != nil || response != "PONG" {
		logger.Error("redis connect fail [", redisHost, " ", redisPort, " ", redisAuth, response, err)
		return errors.New("redis connect fail")
	}
	p.topic = ctx.Get("topic").MustString()
	if p.topic == "" {
		return errors.New("topic empty")
	}
	return nil
}
