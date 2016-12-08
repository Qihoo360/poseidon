package job

import (
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"github.com/garyburd/redigo/redis"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

func readFile(path string) (string, error) {
	fi, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fi.Close()
	fd, err := ioutil.ReadAll(fi)
	return string(fd), nil
}

func readGzFile(path string) (string, error) {
	fi, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fi.Close()

	gz, err := gzip.NewReader(fi)
	if err != nil {
		return "", err
	}
	defer gz.Close()
	s, err := ioutil.ReadAll(gz)
	if err != nil {
		return "", err
	}
	return string(s), nil
}

func sha1Sum(str string) string {
	h := sha1.New()
	io.WriteString(h, str)
	return hex.EncodeToString(h.Sum(nil))
}

func BKDRHash(str string) int {
	seed := 131 // 31 131 1313 13131 131313 etc..
	hash := 0
	strb := []byte(str)
	for _, ch := range strb {
		hash = hash*seed + int(ch)
	}
	return (hash & 0x7FFFFFFF)
}

//message queue
type MessageQueue struct {
	conn redis.Conn
}

//@todo init redis
func NewMessageQueue(c redis.Conn) *MessageQueue {
	return &MessageQueue{
		conn: c,
	}
}

//add queue
func (mq *MessageQueue) AddQueue(name, value string) error {
	_, err := redis.Bool(mq.conn.Do("LPUSH", name, value))
	return err
}

//get queue
func (mq *MessageQueue) GetQueue(name string) (string, error) {
	s, err := redis.String(mq.conn.Do("RPOP", "name"))
	return s, err
}

func FilePutContentAppend(file string, content string) error {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, os.ModeAppend|0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.WriteString(content); err != nil {
		return err
	}
	return nil
}

//根据hash算法获取Collector的index
func getCollectorIndex(hash int, chanSize int, redisSize int) int {
	if chanSize < redisSize {
		// this should never happen, checks must be done during init
		return 0
	}
	tmp := []int{}
	for i := hash % redisSize; i < chanSize; i += redisSize {
		tmp = append(tmp, i)
	}
	return tmp[rand.Intn(len(tmp))]
}

func getRedisIndexFromRule(hash, redisSize int) int {
	return hash % redisSize
}

// 得到len()最小的channel，做负载均衡使用
func GetLeastBusyChannel(channels [](chan Item)) int {
	if len(channels) <= 0 {
		return -1
	}
	index := 0
	length := len(channels[0])
	for i, channel := range channels {
		if len(channel) < length {
			index = i
			length = len(channel)
		}
	}
	return index
}

func getIdcFromHost(host string) string {
	strArr := strings.Split(host, ".")
	if len(strArr) < 3 {
		return ""
	}
	return strArr[2]
}

func ruleIdHash(str string) int {
	i, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return i
}

func bKDRHash(str string) int {
	seed := 131 // 31 131 1313 13131 131313 etc..
	hash := 0
	strb := []byte(str)
	for _, ch := range strb {
		hash = hash*seed + int(ch)
	}
	return (hash & 0x7FFFFFFF)
}
