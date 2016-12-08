package module

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/zieckey/simgo"
)

type IdGenerator struct {
	RedisPool *redis.Pool
}

func New() *IdGenerator {
	m := &IdGenerator{}
	return m
}

func (m *IdGenerator) Initialize() error {
	fw := simgo.DefaultFramework
	redisAddr, _ := fw.Conf.SectionGet("/service/idgenerator", "redis_address")
	redisPass, _ := fw.Conf.SectionGet("/service/idgenerator", "redis_password")
	if len(redisAddr) == 0 || len(redisPass) == 0 {
		return fmt.Errorf("redis_address or redis_password config error")
	}

	m.RedisPool = NewPool(redisAddr, redisPass, IdleTimeout, MaxIdle, MaxActive)

	// http://127.0.0.1:9360/service/idgenerator?count=135&day=20160229&business_name=test
	simgo.HandleFunc("/service/idgenerator", m.GetIdHandler, m).Methods("GET")

	return nil
}

func (m *IdGenerator) Uninitialize() error {
	return nil
}

func (m *IdGenerator) GetIdHandler(w http.ResponseWriter, r *http.Request) {
	now := time.Now().Format("20060102")
	count, _ := strconv.Atoi(r.FormValue("count"))
	if count <= 0 {
		count = 1
	}
	day := r.FormValue("day")
	if len(day) == 0 {
		day = now
	}
	business_name := r.FormValue("business_name")
	if business_name == "" {
		w.WriteHeader(403)
		w.Write([]byte("business_name missing"))
		return
	}

	increaseNum, err := m.GetId(count, business_name, day)
	if err != nil {
		panic(err)
	}

	ret := map[string]interface{}{
		"errno":       0,
		"errmsg":      "",
		"start_index": increaseNum - int64(count),
		"count":       count,
		"time":        time.Now().Unix(),
	}

	b, _ := json.Marshal(ret)
	w.Write(b)
}

const TTL = 604800 //key expire time one week

func (m *IdGenerator) GetId(count int, business_name, day string) (int64, error) {
	conn := m.RedisPool.Get()
	defer conn.Close()
	key := getKey(business_name, day)
	countNum, err := redis.Int64(conn.Do("INCRBY", key, count))
	conn.Do("EXPIRE", key, TTL)
	return countNum, err
}

/**
 * get redis INCRBY key
 */
func getKey(business_name, day string) string {
	bf := bytes.NewBufferString(business_name)
	bf.WriteString("_")
	bf.WriteString(day)
	return bf.String()
}
