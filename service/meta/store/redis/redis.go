// Package redis for store provider
//
// depend on github.com/garyburd/redigo/redis
//
// go install github.com/garyburd/redigo/redis
//
// Usage:
// import(
//     _ "github.com/Qihoo360/poseidon/service/meta/store/redis"
//     "github.com/Qihoo360/poseidon/service/meta/store"
// )
//
// c := store.Config{
//     Timeout: time.Duration(*timeout) * time.Second,
//     Password: *password,
//     Addr: *addr,
// }
//
// db, err = store.NewStore("redis", c)
//
// v = db.Get(...)

package redis

import (
	"github.com/Qihoo360/poseidon/service/meta/store"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
)

// Store is Redis store adapter.
type Redis struct {
	p      *redis.Pool // redis connection pool
	dbNum  int         // redis db number
	config store.Config
}

// NewRedisCache create new redis cache with default collection name.
func NewRedisStore(c store.Config) (store.Store, error) {
	rc := &Redis{config: c}
	rc.connectInit()
	return rc, nil
}

// actually do the redis command
func (rc *Redis) do(commandName string, args ...interface{}) (interface{}, error) {
	c := rc.p.Get()
	defer c.Close()

	return c.Do(commandName, args...)
}

// Get gets a value from redis with the giving key
func (rc *Redis) Get(key string) (r store.GetResult) {
	v, err := rc.do("GET", key)
	r.Value, r.Err = redis.String(v, err)
	if r.Err == redis.ErrNil {
		r.Err = nil
	}
	return
}

// MultiGet gets values from redis with the giving keys
func (rc *Redis) MultiGet(keys []string) map[string] /*the-key*/ store.GetResult {
	size := len(keys)
	rv := make(map[string] /*the-key*/ store.GetResult)
	c := rc.p.Get()
	defer c.Close()
	var err error
	for _, key := range keys {
		err = c.Send("GET", key)
		if err != nil {
			goto ERROR
		}
	}
	if err = c.Flush(); err != nil {
		goto ERROR
	}
	for i := 0; i < size; i++ {
		var r store.GetResult
		if v, err := c.Receive(); err == nil {
			r.Err = nil
			if v == nil {
				//r.Value = nil
			} else {
				r.Value = string(v.([]byte))
			}
			rv[keys[i]] = r
		} else {
			if err != redis.ErrNil {
				glog.Errorf("Redis.MultiGet ERROR : %v", err.Error())
				r.Err = err
			}
			rv[keys[i]] = r
		}
	}
	return rv

ERROR:
	rv = make(map[string] /*the-key*/ store.GetResult)
	for i := 0; i < size; i++ {
		var r store.GetResult
		r.Err = err
		rv[keys[i]] = r
	}

	return rv
}

// Set sets a value with the key into redis.
func (rc *Redis) Set(key string, val string) error {
	var err error
	if _, err = rc.do("SET", key, val); err != nil {
		return err
	}

	return err
}

// Set sets a value with the key into redis.
func (rc *Redis) Incrby(key string, val int64) error {
	var err error
	if _, err = rc.do("INCRBY", key, val); err != nil {
		return err
	}

	return err
}

// Delete deletes key in redis.
func (rc *Redis) Delete(key string) error {
	_, err := rc.do("DEL", key)
	return err
}

// connect to redis.
func (rc *Redis) connectInit() {
	dialFunc := func() (c redis.Conn, err error) {
		c, err = redis.Dial("tcp", rc.config.Addr)
		if err != nil {
			return nil, err
		}

		if rc.config.Password != "" {
			if _, err := c.Do("AUTH", rc.config.Password); err != nil {
				c.Close()
				return nil, err
			}
		}

		_, err = c.Do("SELECT", rc.dbNum)
		if err != nil {
			c.Close()
			return nil, err
		}
		return
	}

	// initialize a new pool
	rc.p = &redis.Pool{
		MaxIdle:     10,
		MaxActive: 100,
		Wait: true,
		IdleTimeout: rc.config.Timeout * 10,
		Dial:        dialFunc,
	}
}

func init() {
	store.Register(store.REDIS, NewRedisStore)
}
