package module

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Qihoo360/poseidon/service/meta/store"
	_ "github.com/Qihoo360/poseidon/service/meta/store/memcached"
	_ "github.com/Qihoo360/poseidon/service/meta/store/redis"
	"github.com/golang/glog"
	"github.com/zieckey/simgo"
)

type BackendStore struct {
	storeType string // The backend store type. We support : memcached , redis
	config    store.Config
	db        store.Store
}

type Meta struct {
	backend map[string] /*name*/ *BackendStore
}

func New() *Meta {
	m := &Meta{
		backend: make(map[string] /*name*/ *BackendStore),
	}
	return m
}

func (m *Meta) Initialize() error {
	fw := simgo.DefaultFramework
	sections := fw.Conf.GetAll()
	for k := range sections {
		if strings.Index(k, "/service/meta/backend-store") != 0 {
			continue
		}

		if err := m.initBackendStore(k); err != nil {
			glog.Errorf("initBackendStore(%v) failed : %v", k, err.Error())
			return err
		}
	}

	simgo.HandleFunc("/service/meta/{business}/{metaType:\\bdoc\\b|\\bindex\\b}/get", m.Get, m).Methods("POST")
	simgo.HandleFunc("/service/meta/{business}/{metaType:\\bdoc\\b|\\bindex\\b}/set", m.Set, m).Methods("POST")
	simgo.HandleFunc("/service/meta/{business}/add", m.Add, m).Methods("POST")

	return nil
}

func (m *Meta) Uninitialize() error {
	return nil
}

func (m *Meta) initBackendStore(section string) error {
	// [/service/meta/backend-store/doc/mobile]
	// business_name = mobile
	// timeout_ms = 3000
	// address = 192.168.0.37:10001
	// store_type = redis

	fw := simgo.DefaultFramework

	n := strings.Trim(section, "/")
	nn := strings.Split(n, "/")
	if len(nn) != 5 || (nn[3] != "doc" && nn[3] != "index") {
		log := fmt.Sprintf("%s format wrong. The right format is /service/meta/backend-store/doc/business_name or /service/meta/backend-store/index/business_name", section)
		glog.Error(log)
		return errors.New(log)
	}

	timeout_ms, _ := fw.Conf.SectionGetInt(section, "timeout_ms")

	backend := &BackendStore{}
	backend.config.Password, _ = fw.Conf.SectionGet(section, "password")
	backend.config.Addr, _ = fw.Conf.SectionGet(section, "address")
	backend.storeType, _ = fw.Conf.SectionGet(section, "store_type")
	backend.config.Timeout = time.Duration(timeout_ms) * time.Millisecond

	if backend.storeType != store.REDIS && backend.storeType != store.MEMCACHED {
		log := fmt.Sprintf("%s format wrong. The right format is /service/meta/backend-store/doc/business_name or /service/meta/backend-store/index/business_name", section)
		glog.Error(log)
		return errors.New(log)
	}

	db, err := store.NewStore(backend.storeType, backend.config)
	if err != nil {
		glog.Errorf("Create backend store failed : %v", err.Error())
		return err
	}
	backend.db = db

	metaType := nn[3]
	businessName := nn[4]
	name := getBackendStoreName(metaType, businessName)
	m.backend[name] = backend
	glog.Infof("initBackendStore %v with type %v", name, backend.storeType)
	return nil
}
