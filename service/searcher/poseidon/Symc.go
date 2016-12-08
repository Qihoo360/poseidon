package poseidon

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

func SymcGet(symcurl string, keys map[string]string) (map[string]string, error) {
	var err error
	log.Println(symcurl)
	buf := bytes.NewBufferString("")
	for key, _ := range keys {
		buf.WriteString(key)
		buf.WriteString("\n")
	}
	log.Println(symcurl)
	req, err := http.NewRequest("POST", symcurl, buf)
	client := &http.Client{
		Timeout: time.Duration(60 * time.Second),
	}

	result, err := client.Do(req)
	if err != nil {
		log.Println("symc error", err.Error())
		return nil, err
	}
	defer result.Body.Close()

	re, err := ioutil.ReadAll(result.Body)
	log.Println(string(re))
	if err != nil {
		return nil, err
	}

	restring := string(re)
	m := make(map[string]string, 50) //预分配50
	strslice := strings.Split(restring, "\n")

	if strslice != nil {
		for _, item := range strslice {
			if item == "" {
				continue
			}
			kvslice := strings.Split(item, "\t")
			key := kvslice[0]
			value, err := base64.StdEncoding.DecodeString(kvslice[1])
			if err != nil {
				continue
			}
			m[key] = string(value)
		}
	}
	log.Println(m)
	return m, nil
}
