package main

import (
	"bytes"
	"encoding/base64"
	"flag"
	"github.com/golang/glog"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

func SetRequest(client *http.Client, k int, batch int, url string) {

	// 1. Construct a request body
	w := bytes.NewBuffer([]byte(""))
	for v := 0; v < batch; v++ {
		key := []byte(strconv.Itoa(k*batch + v))
		w.Write(key)
		w.Write([]byte("\t"))
		value := base64.StdEncoding.EncodeToString(key)
		w.Write([]byte(value))
		w.Write([]byte("\n"))
	}

	// 2. Send SET request
	r, err := http.NewRequest("POST", url, bytes.NewReader(w.Bytes()))
	response, err := client.Do(r)
	if err != nil {
		glog.Errorf("k=%v set failed : %v", k, err.Error())
		return
	}

	// 3. Parse and validate the SET response
	body, _ := ioutil.ReadAll(response.Body)
	keys := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(keys) != batch {
		glog.Errorf("k=%v response key count %v not equal to batch %v", k, len(keys), batch)
		return
	}

	for v, row := range keys {
		rr := strings.Split(row, "\t")
		key := rr[0]
		ok := rr[1]
		rk := strconv.Itoa(k*batch + v)
		if rk != key {
			glog.Errorf("k=%v response key is %v not equal to request %v", k, rk, key)
		}

		if ok != "OK" {
			glog.Errorf("k=%v response key is %v , Status is no OK [%v]", k, rk, ok)
		}
	}
}

func GetRequest(client *http.Client, k int, batch int, url string) {
	// 1. Construct a request body
	w := bytes.NewBuffer([]byte(""))
	for v := 0; v < batch; v++ {
		w.Write([]byte(strconv.Itoa(k*batch + v)))
		w.Write([]byte("\n"))
	}

	// 2. Send GET request
	r, err := http.NewRequest("POST", url, bytes.NewReader(w.Bytes()))
	response, err := client.Do(r)
	if err != nil {
		glog.Errorf("k=%v set failed : %v", k, err.Error())
		return
	}

	// 3. Parse and validate the GET response
	body, _ := ioutil.ReadAll(response.Body)
	keys := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(keys) != batch {
		glog.Errorf("k=%v response key count %v not equal to batch %v", k, len(keys), batch)
		return
	}

	for _, row := range keys {
		rr := strings.Split(row, "\t")
		key := rr[0]
		val, err := base64.StdEncoding.DecodeString(rr[1])
		if err != nil {
			glog.Errorf("k=%v response value base64 decode failed %v", k, err.Error())
			continue
		}

		if string(val) != key {
			glog.Errorf("k=%v response key is [%v] val is [%v]", k, key, string(val))
			continue
		}
	}
}

func main() {
	d := flag.Duration("d", time.Second*30, "The duration time")
	c := flag.Int("c", 1, "The concurrency connections")
	u := flag.String("u", "http://127.0.0.1:39610/service/meta/test/doc", "The url")

	flag.Parse()

	timer := time.NewTimer(*d)

	running := true

	var wg sync.WaitGroup
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var client http.Client
			for k := 0; running; k++ {
				batch := 10
				SetRequest(&client, k, batch, *u+"/set")
				GetRequest(&client, k, batch, *u+"/get")
				glog.Infof("k=%v test OK", k)
			}
		}()
	}

	<-timer.C
	running = false
	wg.Wait()
}
