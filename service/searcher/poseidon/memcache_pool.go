// package main
package poseidon

import (
	"errors"
	"github.com/bradfitz/gomemcache/memcache"
	"sync"
	"time"
)

var gAddrMemcachePool = AddrMemcachePool{}

type AddrMemcachePool struct {
	poolMap map[string]*MemcachePool
	sync.Mutex
}

func (self *AddrMemcachePool) Init() {
	self.poolMap = make(map[string]*MemcachePool)
}

func (self *AddrMemcachePool) Get(addr string) *MemcachePool {
	self.Lock()
	defer self.Unlock()
	if pool, ok := self.poolMap[addr]; ok {
		return pool
	}
	pool := NewPoolInstance(addr, 50)
	self.poolMap[addr] = pool
	return pool
}

func init() {
	gAddrMemcachePool.Init()
}

func NewPoolInstance(addr string, limit int) *MemcachePool {
	var memcachePool = MemcachePool{addr: addr, limit: limit}
	return &memcachePool
}

type MemcachePool struct {
	// TODO: add lcoal cache
	pool []*memcache.Client
	sync.Mutex

	addr      string
	allocated int
	limit     int
}

func (self *MemcachePool) Alloc() (client *memcache.Client, err error) {
	self.Lock()

	if len(self.pool) > 0 {
		client := self.pool[0]
		self.pool = self.pool[1:]
		self.allocated++
		self.Unlock()
		return client, nil
	}

	if self.allocated >= self.limit {
		self.Unlock()
		return nil, errors.New("too_many_conns")
	}

	self.allocated++
	self.Unlock()

	c := memcache.New(self.addr)
	c.Timeout = 300 * time.Millisecond
	return c, nil
}

func (self *MemcachePool) Release(client *memcache.Client, success bool) {
	if !success {
		// client.DeleteAll()
	}

	self.Lock()
	defer self.Unlock()
	self.allocated--

	if success {
		self.pool = append(self.pool, client)
	}
}
