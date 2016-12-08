package store

import (
	"fmt"
	"time"
)

type GetResult struct {
	Err   error
	Value string
}

type Store interface {
	// Get gets a value by key
	// If the key is not found in the store, the Err is nil and Value is empty
	Get(key string) GetResult

	// MultiGet is a batch version of Get
	MultiGet(keys []string) map[string] /*the-key*/ GetResult

	// Set sets a value with key where the key exists or not
	Set(key, value string) error

	// Delete deletes a key/value pair from the store
	Delete(key string) error
}

// Config contains the options for a storage client
type Config struct {
	Timeout  time.Duration
	Addr     string // The address of the backend store with "host:port" format
	Password string // The authority password if necessarily
}

// The backend store name
const (
	MEMCACHED string = "memcached"
	REDIS     string = "redis"
)

// StoreCreator is a function to create a new instance of Store
type StoreCreator func(Config) (Store, error)

var creators = make(map[string]StoreCreator)

// Register makes a store creator available by name.
func Register(name string, creator StoreCreator) {
	if creator == nil {
		panic("cache: Register adapter is nil")
	}
	if _, ok := creators[name]; ok {
		panic("cache: Register called twice for adapter " + name)
	}
	creators[name] = creator
}

// NewStore creates a new store driver by store name and configurations.
func NewStore(name string, c Config) (db Store, err error) {
	creator, ok := creators[name]
	if !ok {
		err = fmt.Errorf("unknown store name [%v] (forgot to import?)", name)
		return
	}
	db, err = creator(c)
	if err != nil {
		db = nil
	}
	return db, err
}
