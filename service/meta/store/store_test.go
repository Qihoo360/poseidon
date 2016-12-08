package store_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/Qihoo360/poseidon/service/meta/store"
	_ "github.com/Qihoo360/poseidon/service/meta/store/memcached"
	_ "github.com/Qihoo360/poseidon/service/meta/store/redis"
)

func testStore(s store.Store, t *testing.T) {
	var err error
	if r := s.Get("poseidon"); r.Err != nil || r.Value != "" {
		t.Errorf("Get ERROR: %v [%v]", r.Err.Error(), r.Value)
	}

	if err = s.Set("poseidon", "1"); err != nil {
		t.Error("Set ERROR", err)
	}

	if r := s.Get("poseidon"); r.Err != nil || r.Value != "1" {
		t.Error("Get ERROR")
	}

	if err = s.Set("poseidon", "2"); err != nil {
		t.Error("Set ERROR", err)
	}

	if r := s.Get("poseidon"); r.Err != nil || r.Value != "2" {
		t.Error("Get ERROR")
	}

	s.Delete("poseidon")

	if r := s.Get("poseidon"); r.Err != nil || r.Value != "" {
		t.Errorf("Get ERROR: %v [%v]", r.Err.Error(), r.Value)
	}

	var keys []string = []string{"poseidon0", "poseidon1", "poseidon2", "poseidon3", "poseidon4", "poseidon5"}

	for i, key := range keys {
		v := strconv.Itoa(i)
		if err = s.Set(key, v); err != nil {
			t.Error("Set ERROR", err)
		}

		if r := s.Get(key); r.Err != nil || r.Value != v {
			t.Error("Get ERROR")
		}
	}

	rr := s.MultiGet(keys)
	for k, r := range rr {
		if r.Err != nil || string("poseidon")+r.Value != k {
			t.Errorf("MultiGet ERROR, [%v] [%v] [%v]", r.Err, k, r.Value)
		}
	}

	for _, key := range keys {
		if err = s.Delete(key); err != nil {
			t.Error("Set ERROR", err)
		}

		if r := s.Get(key); r.Err != nil || r.Value != "" {
			t.Error("Get ERROR")
		}
	}
}

func TestRedisStore(t *testing.T) {
	c := store.Config{
		Addr:     "127.0.0.1:45678",
		Timeout:  time.Duration(1) * time.Second,
		Password: "your_pass",
	}

	s, err := store.NewStore(store.REDIS, c)

	if err != nil {
		t.Error("init err")
	}

	testStore(s, t)
}

func TestMemcachedStore(t *testing.T) {
	c := store.Config{
		Addr:    "127.0.0.1:27000",
		Timeout: time.Duration(1) * time.Second,
	}

	s, err := store.NewStore(store.MEMCACHED, c)

	if err != nil {
		t.Error("init err")
	}

	testStore(s, t)
}
