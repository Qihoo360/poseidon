package simgo

import (
    "errors"
    "flag"
    "fmt"
    "io/ioutil"
    "net/http"
    "os"
    "os/signal"
    "strconv"
    "sync"
    "syscall"

    "github.com/golang/glog"
    "github.com/gorilla/mux"
    "github.com/zieckey/dbuf"
    "github.com/zieckey/goini"
    "time"
)

// The default unis.instance
var DefaultFramework = &duxFramework
var duxFramework Framework

type Framework struct {
    Conf           *goini.INI
    ConfigPath     string
    DBufManager    *dbuf.Manager
    Router         *mux.Router
    ReadTimeout    time.Duration
    WriteTimeout   time.Duration

    debug          bool
    httpAddr       string            // The http server listen address
    modules        map[string]Module // map<module_name, Module>
    accessLog      bool
    statusFilePath string            // The status.html file path
    server         *http.Server
}

func init() {
    duxFramework.modules = make(map[string]Module)
    duxFramework.accessLog = true
    duxFramework.debug = false
}

// RegisterModule 会将应用层写的模块注册到框架中。注意必须在Run/Initialize等方法之前调用该函数
func (fw *Framework) RegisterModule(name string, m Module) error {
    if _, ok := fw.modules[name]; ok {
        return errors.New(name + " module arready exists!")
    }

    fw.modules[name] = m
    return nil
}

// Initialize 框架初始化，在RegisterModule之后调用
func (fw *Framework) Initialize() error {
    if !flag.Parsed() {
        flag.Parse()
    }

    fw.DBufManager = dbuf.NewManager()

    configFilePath := *ConfPath
    fw.ConfigPath = configFilePath
    ini, err := goini.LoadInheritedINI(configFilePath)
    if err != nil {
        return errors.New("parse INI config file error : " + configFilePath)
    }
    fw.Conf = ini

    fw.debug, _ = fw.Conf.SectionGetBool("common", "debug")
    fw.accessLog, _ = fw.Conf.SectionGetBool("common", "access_log")

    httpPort, _ := fw.Conf.SectionGet("common", "http_port")
    if len(httpPort) == 0 {
        return errors.New("Not found communication port")
    }
    if len(httpPort) > 0 {
        fw.httpAddr = fmt.Sprintf(":%v", httpPort)
    }

    fw.statusFilePath = fw.getPathConfig("common", "monitor_status_file_path")

    timeout, _ := fw.Conf.SectionGetInt("common", "http_read_timeout_ms")
    if timeout < 1 {
        timeout = 100
    }
    fw.ReadTimeout = time.Duration(timeout) * time.Millisecond

    timeout, _ = fw.Conf.SectionGetInt("common", "http_write_timeout_ms")
    if timeout < 1 {
        timeout = 100
    }
    fw.WriteTimeout = time.Duration(timeout) * time.Millisecond

    fw.Router = mux.NewRouter()

    return nil
}

// Run 会启动 server 进入监听状态
func (fw *Framework) Run() {

    // register internal modules
    fw.RegisterModule("monitor", new(MonitorModule))
    fw.RegisterModule("admin", new(AdminModule))

    // register business modules
    for name, module := range fw.modules {
        err := module.Initialize()
        if err != nil {
            log := name + " module initialized failed : " + err.Error()
            glog.Errorf("%v", log)
            panic(log)
        }
    }

    var wg sync.WaitGroup

    wg.Add(1)
    fw.watchSignal(&wg)

    wg.Add(1)
    go fw.runHTTP(&wg)

    // Create PID file now
    fw.createPidFile()
    defer fw.removePidFile()

    wg.Wait()
}

func (fw *Framework) runHTTP(wg *sync.WaitGroup) {
    defer wg.Done()
    glog.Warningf("Running http service at %v", fw.httpAddr)
    fw.server = &http.Server{
        Addr: fw.httpAddr,
        Handler: fw.Router,
        ReadTimeout: fw.ReadTimeout,
        WriteTimeout: fw.WriteTimeout,
    }
    if err := fw.server.ListenAndServe(); err != nil {
        glog.Errorf("Run HTTP server failed : %v", err.Error())
    }
}

func (fw *Framework) watchSignal(wg *sync.WaitGroup) {
    defer wg.Done()

    // Set up channel on which to send signal notifications.
    c := make(chan os.Signal, 1)
    signal.Notify(c)

    // Block until a signal is received.
    go func() {
        defer close(c)
        for {
            s := <-c
            glog.Errorf("Got signal %v", s)
            if s == syscall.SIGHUP || s == syscall.SIGINT || s == syscall.SIGTERM {
                for name, module := range fw.modules {
                    err := module.Uninitialize()
                    if err != nil {
                        glog.Errorf("%v module Uninitialize failed : %v", name, err.Error())
                    }
                }
                signal.Stop(c)
                os.Exit(0) // TODO how to stop HTTP Server and exit gracefully
            }
        }
    }()
}

// GetPathConfig 获取一个路径配置项的相对路径（相对于 ConfPath 而言）
// e.g. :
// 		ConfPath = /home/simgo/conf/app.ini
//
//	and the app.conf has a config item as below :
//  	[business]
//		qlog_conf = qlog.conf
//
// and then the GetPathConfig("business", "qlog_conf") will
// return /home/simgo/conf/qlog.conf
func (fw *Framework) getPathConfig(section, key string) string {
    filepath, ok := fw.Conf.SectionGet(section, key)
    if !ok {
        println(key + " config is missing in " + section)
        return ""
    }
    return goini.GetPathByRelativePath(fw.ConfigPath, filepath)
}

func (fw *Framework) createPidFile() {
    pidPath := fw.getPathConfig("common", "pid_file")
    pid := os.Getpid()
    pidString := strconv.Itoa(pid)
    if err := ioutil.WriteFile(pidPath, []byte(pidString), 0644); err != nil {
        panic("Create pid file failed : " + pidPath)
    }
    glog.Infof("Create pid file : %v", pidPath)
}

func (fw *Framework) removePidFile() {
    pidPath := fw.getPathConfig("common", "pid_file")
    os.Remove(pidPath)
    println("remove pid file : ", pidPath)
}
