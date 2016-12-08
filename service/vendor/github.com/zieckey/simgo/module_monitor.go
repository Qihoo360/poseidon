package simgo

import (
    "fmt"
    "os"
    "bufio"
    "strings"
    "io"
    "net/http"
    _ "net/http/pprof"

    "github.com/golang/glog"
)

const (
    StatusOK = "OK"
    QPollerOK = "ok"
    MAINTAIN = "MAINTAIN"
    FAILED = "FAILED"
)

type MonitorModule struct {
}

func (m *MonitorModule) Initialize() error {
    HandleFunc("/status.html", m.Status, m).Methods("GET")
    HandleFunc("/qpoller/status.html", m.Status, m).Methods("GET")
    return nil
}

func (m *MonitorModule) Uninitialize() error {
    return nil
}

func (m *MonitorModule) Status(w http.ResponseWriter, req *http.Request) {
    file, err := os.Open(duxFramework.statusFilePath)
    if err != nil {
        fmt.Printf("ERROR open file <%v> failed : %v\n", duxFramework.statusFilePath, err.Error())
        w.Write([]byte(FAILED))
        return
    }
    defer file.Close()

    r := bufio.NewReader(file)
    line, err := r.ReadString('\n')
    if err != nil && err != io.EOF {
        fmt.Printf("ERROR read the first line from file <%v> failed : %v\n", duxFramework.statusFilePath, err.Error())
        w.Write([]byte(FAILED))
        return
    }

    line = strings.TrimSpace(line)
    if strings.ToLower(line) == "ok" {
        if req.URL.Path == "/status.html" {
            w.Write([]byte(StatusOK))
            return
        } else if req.URL.Path == "/qpoller/status.html" {
            w.Write([]byte(QPollerOK))
            return
        } else {
            w.Write([]byte(StatusOK))
            return
        }
    }

    if strings.ToUpper(line) == MAINTAIN {
        w.Write([]byte(MAINTAIN))
        return
    }

    glog.Errorf("ERROR the first line from file <%v> format wrong: [%v]\n", duxFramework.statusFilePath, line)
    w.Write([]byte(FAILED))
}
