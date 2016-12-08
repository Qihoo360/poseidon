package main

import (
	proxy "github.com/Qihoo360/poseidon/service/proxy/module"
	"github.com/zieckey/simgo"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

var pidfile string = "./pid"

func main() {
	os.Chdir(path.Dir(os.Args[0]))
	fw := simgo.DefaultFramework
	fw.RegisterModule("proxy_service", proxy.New())
	managePid(true, pidfile)
	err := fw.Initialize()
	if err != nil {
		panic(err.Error())
	}

	fw.Run()
}

//生成/删除当前进程id文件
func managePid(create bool, pidfile string) {
	if pidfile == "" {
		panic("pidfile is null, please configure it")
	}
	if create {
		pid := os.Getpid()
		pidString := strconv.Itoa(pid)
		ioutil.WriteFile(pidfile, []byte(pidString), 0777)
	} else {
		os.Remove(pidfile)
	}
}
