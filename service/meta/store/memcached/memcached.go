package memcached

import (
	"github.com/Qihoo360/poseidon/service/meta/store"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/glog"
)

type Memcached struct {
	conn   *memcache.Client
	config store.Config
}

func NewMemcachedStore(c store.Config) (store.Store, error) {
	rc := &Memcached{
		config: c,
	}

	err := rc.connectInit()
	if err == nil {
		return rc, nil
	}

	return nil, err
}

func (rc *Memcached) Get(key string) (result store.GetResult) {
	if item, err := rc.conn.Get(key); err == nil {
		result.Value = string(item.Value)
	} else {
		if err != memcache.ErrCacheMiss {
			result.Err = err
		}
	}
	return result
}

func (rc *Memcached) MultiGet(keys []string) map[string] /*the-key*/ store.GetResult {
	size := len(keys)
	rv := make(map[string] /*the-key*/ store.GetResult)
	mv, err := rc.conn.GetMulti(keys)
	if err == nil {
		for _, v := range mv {
			var r store.GetResult
			r.Value = string(v.Value)
			glog.Infof("Memcached.MultiGet key=[%v] value=[%v]", v.Key, r.Value)
			rv[v.Key] = r
		}
		return rv
	} else {
		glog.Errorf("Memcached.MultiGet ERROR : %v", err.Error())
	}

	for i := 0; i < size; i++ {
		var r store.GetResult
		if err != memcache.ErrCacheMiss {
			r.Err = err
		}
		rv[keys[i]] = r
	}
	return rv
}

func (rc *Memcached) Set(key, value string) error {
	item := memcache.Item{Key: key, Value: []byte(value)}
	return rc.conn.Set(&item)
}

func (rc *Memcached) Incrby(key string, value int) error {
	var err error
	return err
}

func (rc *Memcached) Delete(key string) error {
	return rc.conn.Delete(key)
}

func (rc *Memcached) connectInit() error {
	rc.conn = memcache.New(rc.config.Addr)
	return nil
}

func init() {
	store.Register(store.MEMCACHED, NewMemcachedStore)
}
