package simgo

import (
    "fmt"
    _ "net/http/pprof"
    "github.com/golang/glog"
    "net/http"
)

type AdminModule struct {
}

func (m *AdminModule) Initialize() error {
    HandleFunc("/admin/reload", m.Reload, m).Queries("name", "", "path", "")
    return nil
}

func (m *AdminModule) Uninitialize() error {
    return nil
}

func (m *AdminModule) Reload(w http.ResponseWriter, r *http.Request) {
    name := r.URL.Query().Get("name")
    path := r.URL.Query().Get("path")

    glog.Info("url=[%v] name=[%v] path=[%v]\n", r.URL.String(), name, path)

    if len(name) == 0 {
        w.Write([]byte(fmt.Sprint("parameter 'name' ERROR, URI=[%v]", r.URL.String())))
        return
    }

    if len(path) == 0 {
        w.Write([]byte(fmt.Sprint("parameter 'path' ERROR, URI=[%v]", r.URL.String())))
        return
    }

    err := duxFramework.DBufManager.Reload(name, path)
    if err == nil {
        w.Write([]byte("OK"))
        return
    }

    w.Write([]byte(fmt.Sprintf("Reload name=%s path=%s failed : %v", name, path, err.Error())))
}
