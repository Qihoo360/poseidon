package simgo

import (
    "net/http"
    "time"
    "reflect"
    "strings"

    "github.com/golang/glog"
    "github.com/gorilla/mux"
)

type HandlerFunc func(w http.ResponseWriter, r *http.Request)

type Handler struct {
    n string // the name of the module
    m Module
    f HandlerFunc
}


// HandleFunc registers a new route with a matcher for the URL path.
// See Route.Path() and Route.HandlerFunc().
func HandleFunc(path string, f func(http.ResponseWriter, *http.Request), m Module) *mux.Route {
    h := &Handler{
        n: getModuleName(m),
        m: m,
        f: f,
    }

    return duxFramework.Router.HandleFunc(path, h.serveHTTP)
}

func (h *Handler) serveHTTP(w http.ResponseWriter, r *http.Request) {
    beginTime := time.Now()
    if duxFramework.debug {
        glog.Infof("%v url=[%v]", h.n, r.URL.String())
    }

    h.f(w, r)
    costMs := float64(time.Since(beginTime).Nanoseconds()) / 1000000.0
    if duxFramework.accessLog {
        glog.Warningf("%v\t%v\tcost:%v", r.RemoteAddr, r.URL.String(), costMs)
    }
}

// Get the name of m using reflect mechanism
func getModuleName(m Module) string {
    n := reflect.ValueOf(m).Elem().String() // n = "<simgo.MonitorModule Value>"
    n = strings.Trim(n, "><") // n = "simgo.MonitorModule Value"
    n = strings.Split(n, " ")[0] // n = "simgo.MonitorModule"
    return n
}
