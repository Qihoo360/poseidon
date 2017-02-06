package module

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Qihoo360/poseidon/service/meta/store"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
)

func getBackendStoreName(metaType, businessName string) string {
	return metaType + "/" + businessName
}

func (m *Meta) Get(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	metaType := vars["metaType"]
	business := vars["business"]
	//glog.Infof("Get metaType=%v business=%v key=%v", metaType, business,
	//	getBackendStoreName(metaType, business));
	var db store.Store
	if v, ok := m.backend[getBackendStoreName(metaType, business)]; ok {
		db = v.db
	} else {
		glog.Errorf("backend store name=%v NOT FOUND", getBackendStoreName(metaType, business))
		w.WriteHeader(403)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		glog.Errorf("Read http body failed : %v", err.Error())
		w.WriteHeader(500)
		return
	}

	keys := strings.Split(string(body), "\n")
	results := db.MultiGet(keys)
	for k, r := range results {
		w.Write([]byte(k))
		w.Write([]byte("\t"))
		if r.Err == nil {
			w.Write([]byte(base64.StdEncoding.EncodeToString([]byte(r.Value))))
			glog.Infof("db.MultiGet key=[%v] value=[%v]", k, base64.StdEncoding.EncodeToString([]byte(r.Value)))
		} else {
			glog.Infof("db.MultiGet key=[%v] failed [%v]", k, r.Err.Error())
		}
		w.Write([]byte("\n"))
	}
}

type Pair struct {
	key string
	v   string
}

func parseRequest(body string) ([]Pair, error) {
	glog.Infof("Body:\n%v", body)
	inputs := strings.Split(strings.TrimSpace(body), "\n")
	kvs := make([]Pair, len(inputs))
	for i, input := range inputs {
		f := func(r rune) bool {
			if r == '\t' || r == ' ' {
				return true
			}
			return false
		}
		k := strings.FieldsFunc(input, f)
		if len(k) == 2 {
			kvs[i].key = k[0]
			v, err := base64.StdEncoding.DecodeString(k[1])
			if err == nil {
				kvs[i].v = string(v)
			} else {
				return nil, fmt.Errorf("row %d error, base64 decode failed [%v]", i, k[1])
			}
		} else {
			return nil, fmt.Errorf("row %d error, cannot split to key and value [%v]", i, input)
		}
	}

	return kvs, nil
}

func (m *Meta) Set(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	vars := mux.Vars(r)
	metaType := vars["metaType"]
	business := vars["business"]

	var db store.Store
	if v, ok := m.backend[getBackendStoreName(metaType, business)]; ok {
		db = v.db
	} else {
		glog.Errorf("backend store name=%v NOT FOUND", getBackendStoreName(metaType, business))
		w.WriteHeader(403)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		glog.Errorf("Read http body failed : %v", err.Error())
		w.WriteHeader(500)
		return
	}

	kvs, err := parseRequest(string(body))
	if err != nil {
		glog.Errorf("Parse request failed : %v", err.Error())
		w.WriteHeader(403)
		return
	}

	results := make([]Pair, len(kvs))
	for i, k := range kvs {
		err := db.Set(k.key, k.v)
		results[i].key = k.key
		if err == nil {
			results[i].v = "OK"
			glog.Infof("Set key=[%v] value=[%v] OK", k.key, k.v)
		} else {
			glog.Errorf("Set key [%v] failed : %v", k.key, err.Error())
			results[i].v = err.Error()
		}
	}

	for _, r := range results {
		w.Write([]byte(r.key))
		w.Write([]byte("\t"))
		w.Write([]byte(r.v))
		w.Write([]byte("\n"))
	}
}
