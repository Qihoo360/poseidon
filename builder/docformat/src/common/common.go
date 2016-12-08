package common

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	sj "github.com/bitly/go-simplejson"
	"github.com/garyburd/redigo/redis"
)

// to manage redis pool,key is host:port,val is redis.Pool
var redisPoolManager RedisPoolManager

type RedisPoolManager struct {
	lock    sync.Mutex
	poolMap map[string]*redis.Pool
}

func init() {
	redisPoolManager.poolMap = make(map[string]*redis.Pool)
}

func GetCtx(file string) (*sj.Json, error) {
	jsonStr, err := ReadFile(file)
	if err != nil {
		return nil, errors.New("read config " + file + " failed")
	}

	if strings.Contains(jsonStr, "{shorthost}") { // shorthost作为预定义的"宏"
		hostName, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		strArr := strings.Split(hostName, ".")
		if len(strArr) <= 0 || len(strArr[0]) <= 0 {
			return nil, errors.New("hostname invalid: " + hostName)
		}
		jsonStr = strings.Replace(jsonStr, "{shorthost}", strArr[0], 1)
	}

	ctx, err := sj.NewJson([]byte(jsonStr))
	if err != nil || ctx == nil {
		return nil, err
	}
	return ctx, nil
}

func ReadFile(path string) (string, error) {
	fi, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	return string(fd), nil
}

func WriteFile(path string, content string) error {
	return ioutil.WriteFile(path, []byte(content), 0644)
}

func GetSubStr(str, pre, post string) string {
	index1 := strings.Index(str, pre)
	if index1 == -1 {
		return ""
	}
	index2 := strings.Index(str[index1+len(pre):], post)
	if index2 == -1 {
		return ""
	}
	return str[index1+len(pre) : index1+len(pre)+index2]
}

func GzCompress(raw string) string {
	buffer := new(bytes.Buffer)
	w := gzip.NewWriter(buffer)
	w.Write([]byte(raw))
	w.Flush()
	w.Close() // cannot use defer. should close before get the string
	//	str := string(buffer.Bytes())
	return buffer.String()
}

func ZlibCompress(raw string) string {
	buffer := new(bytes.Buffer)
	w := zlib.NewWriter(buffer)
	w.Write([]byte(raw))
	w.Flush()
	w.Close() // cannot use defer. should close before get the string
	//	str := string(buffer.Bytes())
	return buffer.String()
}

func ZlibDeCompress(line []byte) (string, error) {
	buffer := bytes.NewBuffer(line)
	if buffer == nil {
		return "", errors.New("create buffer failed")
	}
	r, err := zlib.NewReader(buffer)
	if err != nil {
		return "", errors.New("create zlib reader failed")
	}
	defer r.Close()
	byteResult, err := ioutil.ReadAll(r)
	if err != nil {
		return "", errors.New("read result failed")
	}
	return string(byteResult), nil
}

func FilePutContent(outfile string, content string) error {

	file, err := os.OpenFile(outfile, os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	writer.WriteString(content)
	writer.Flush()

	return err
}

func NewPool(server string, password string, idle int, timeoutSec int) *redis.Pool {
	redisPoolManager.lock.Lock()
	defer redisPoolManager.lock.Unlock()
	if _, ok := redisPoolManager.poolMap[server]; !ok {
		connectTimeout := 30 * time.Second
		readTimeout := 20 * time.Second
		writeTimeout := 20 * time.Second
		redisPoolManager.poolMap[server] = &redis.Pool{
			MaxIdle:     idle,
			IdleTimeout: time.Duration(timeoutSec) * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.DialTimeout("tcp", server, connectTimeout, readTimeout, writeTimeout)
				if err != nil {
					return nil, err
				}
				if password != "" {
					if _, err := c.Do("AUTH", password); err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		}
	}

	return redisPoolManager.poolMap[server]
}

func CheckIdProcessed(conn redis.Conn, hash_key string, key string) (string, error) {
	val, err := redis.String(conn.Do("HGET", hash_key, key))
	if err == redis.ErrNil {
		return "0", nil
	}
	return val, err
}

func SetIdProcessed(conn redis.Conn, hash_key string, key string, val string) (int, error) {
	return redis.Int(conn.Do("HSET", hash_key, key, val))
}

// get 2016-02-26-12 from http://hostname:80/access/info.2016-02-26-12-05.gz
func GetHourStrFromId(id string) (string, error) {
	timeFormat := "2006-01-02-15"
	curYear := strconv.Itoa(time.Now().Year())
	idx := strings.Index(id, curYear+"-")
	if idx < 0 {
		return "", errors.New("path not contain yearStr")
	}

	subStr := id[idx:] // 2016-01-08-13-01.gz or 2016-01-08-13.gz

	if len(subStr) < len(timeFormat) {
		return "", errors.New("time str too short:" + subStr)
	}
	_, err := time.Parse(timeFormat, subStr[0:len(timeFormat)]) // get "2015-12-14-17-00"
	if err != nil {
		return "", err
	}
	return subStr[0:len(timeFormat)], nil
}
