package module

/**
 * 底层redis 连接池
 * @author guojun-s@360.cn
 *
 */

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	PROTOCOL = "tcp" //connection protocol
)

var (
	MaxIdle     int           = 100
	MaxActive   int           = 1000
	IdleTimeout time.Duration = time.Duration(28 * time.Second)
)

//
//var RedisPool *redis.Pool
//var Rp = &Pool{}
//
//type Pool struct{}
//
//func (p *Pool) Init(server string, password string, IdleTimeout time.Duration, MaxIdle, MaxActive int) {
//    RedisPool = NewPool(server, password, IdleTimeout, MaxIdle, MaxActive)
//}

/**
 * Redis Pool
 *
 * serverAddr the server address 127.0.0.1:6379
 * password password 127.0.0.1:6379:password
 * IdleTimeout  超时
 * MaxIdle 连接池最大容量
 * MaxActive 最大活跃数量
 * dbno 选择db127.0.0.1:6379:password:1
 *
 */
func NewPool(serverAddr string, password string, IdleTimeout time.Duration, MaxIdle, MaxActive int) *redis.Pool {

	return &redis.Pool{
		MaxIdle:     MaxIdle,
		MaxActive:   MaxActive,
		IdleTimeout: IdleTimeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(PROTOCOL, serverAddr)
			if err != nil {
				return nil, err
			}

			//校验密码
			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}

			return c, err

		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
