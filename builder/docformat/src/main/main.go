package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"time"

	"common"

	//"github.com/bitly/go-nsq"
	sj "github.com/bitly/go-simplejson"
	"github.com/donnie4w/go-logger/logger"
)

const (
	WONT_USE = iota

	RET_INIT_SUCCESS
	RET_INIT_FAIL

	CMD_EXIT
	RET_EXIT_SUCCESS
	RET_EXIT_FAIL

	CMD_TICK
)

func WatchSignal(functor func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c)

	// Block until a signal is received.
	go func() {
		defer close(c)
		for {
			s := <-c
			logger.Debugf("Got signal %v", s)
			if s == syscall.SIGTERM {
				logger.Debug("Got signal SIGTERM")
				functor()
			}
			if s == syscall.SIGHUP {
				logger.Debug("Got signal SIGHUP, nothing to do")
			}
			if s == syscall.SIGINT {
				logger.Debug("Got signal SIGINT stop now!!!")
				os.Exit(1)
			}
		}
	}()
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: $0 -c config_file [-t]\n")
		os.Exit(1)
	}

	var configFile string
	flag.StringVar(&configFile, "c", "conf.json", "config file in json")
	flag.Parse()
	logger.Debug("conf:", configFile)

	ctx, err := common.GetCtx(configFile)
	if ctx == nil || err != nil {
		panic(err.Error())
	}

	pidFileName := ctx.Get("main").Get("pid_file").MustString("./logs/docformat.pid")
	procCount := ctx.Get("main").Get("max_procs").MustInt(10)
	if procCount <= 10 {
		procCount = 10
	}
	if err := WritePidFile(pidFileName); err != nil {
		panic(err.Error())
	}
	runtime.GOMAXPROCS(procCount)

	if err = InitLog(ctx); err != nil {
		panic(err.Error())
	}

	logger.Infof("NumCPU: %d, Now Max Procs: %d", runtime.NumCPU(), runtime.GOMAXPROCS(0))

	// GC
	go func() {
		memLimit := ctx.Get("main").Get("mem_limit").MustUint64(5000)
		tickSec := ctx.Get("main").Get("tick_sec").MustInt(20)
		ticker := time.NewTicker(time.Duration(tickSec) * time.Second)
		var ms runtime.MemStats
		for {
			select {
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				alloc := ms.Alloc / 1024 / 1024
				logger.Infof("GC NumGoroutine: %d", runtime.NumGoroutine())
				logger.Info("GC memAlloc:", alloc, "M heapAlloc:", ms.HeapAlloc/1024/1024, "M, stackAlloc:",
					ms.StackInuse/1024/1024, "M")
				if alloc >= memLimit {
					debug.FreeOSMemory()
					runtime.ReadMemStats(&ms)
					alloc = ms.Alloc / 1024 / 1024
					logger.Info("GC after GC memAlloc:", alloc, "M heapAlloc:", ms.HeapAlloc/1024/1024,
						"M, stackAlloc:", ms.StackInuse/1024/1024, "M")
				}
			}
		}
	}()

	multiMode := ctx.Get("main").Get("is_multi").MustBool(false)

	if !multiMode {
		// 单workshop模式，之前的模式
		controlChan := make(chan int)
		reportChan := make(chan int)

		var workshop Workshop
		if err := workshop.Init("MAIN", ctx, controlChan, reportChan); err != nil {
			panic(err.Error())
		}

		WatchSignal(func() {
			logger.Info("main receive close cmd")
			controlChan <- CMD_EXIT
			logger.Info("main begin wait for workshop to report")
			ret := <-reportChan
			if ret == RET_EXIT_SUCCESS {
				logger.Info("main workshop exit success")
			} else {
				logger.Info("main workshop exit fail")
			}
			logger.Info("main workshop exit now")
		})

		workshop.Run()
		logger.Info("main workshop Run ended")
	} else {
		workshopInfoMap := map[string]WorkshopInfo{}
		businessList := ctx.Get("main").Get("business_list").MustStringArray()
		config_dir := ctx.Get("main").Get("config_dir").MustString()
		for _, business := range businessList {
			configPath := config_dir + "/" + business + ".json"
			logger.Infof("main business: %s, configPath: %s", business, configPath)
			config, err := common.GetCtx(configPath)
			if config == nil || err != nil {
				logger.Errorf("main workshop config error configPath: %s, err: %v", configPath, err)
				panic(err.Error())
			}
			var workshop Workshop
			controlChan := make(chan int)
			reportChan := make(chan int)
			if err := workshop.Init(business, config, controlChan, reportChan); err != nil {
				logger.Errorf("main workshop Init err: %v", err)
				panic(err.Error())
			}
			workshopInfoMap[business] = WorkshopInfo{
				workshop:    &workshop,
				controlChan: controlChan,
				reportChan:  reportChan,
			}
			logger.Infof("main workshop Init success for %s", business)
		}

		for business, workshopInfo := range workshopInfoMap {
			go workshopInfo.workshop.Run()
			logger.Infof("main Run for %s", business)
		}

		var wg sync.WaitGroup
		wg.Add(1)

		WatchSignal(func() {
			logger.Info("main receive close cmd")
			wg.Done()
		})

		wg.Wait()

		// After kill
		func() {
			logger.Info("wg.Wait() ended now begin exit")
			for business, workshopInfo := range workshopInfoMap {
				workshopInfo.controlChan <- CMD_EXIT
				logger.Infof("main send exit to %s successful", business)
			}

			logger.Infof("main begin wait for workshop to report, len(workshopInfoMap): %d", len(workshopInfoMap))

			for business, workshopInfo := range workshopInfoMap {
				ret := <-workshopInfo.reportChan
				if ret == RET_EXIT_SUCCESS {
					logger.Infof("main workshop exit success for %s", business)
				} else {
					logger.Infof("main workshop exit fail for %s", business)
				}
			}
			logger.Info("main workshop exit now")
		}()
		logger.Info("main workshop Run ended")
	}
}

type WorkshopInfo struct {
	workshop    *Workshop
	controlChan chan int
	reportChan  chan int
}

func InitLog(ctx *sj.Json) error {
	logDir := ctx.Get("main").Get("log_dir").MustString("./logs/")
	logName := ctx.Get("main").Get("log_name").MustString("docformat.log")
	logLevel := ctx.Get("main").Get("log_level").MustInt(1)
	if logLevel < 0 {
		logLevel = 0
	}
	if logLevel > 6 {
		logLevel = 6
	}
	logger.SetRollingDaily(logDir, logName)
	logger.SetLevel(logger.LEVEL(logLevel))

	// 关闭Console输出
	logger.SetConsole(false)
	logger.Debugf("InitLog success, logDir: %s, logName: %s, logLevel: %v", logDir, logName, logLevel)
	return nil
}

func WritePidFile(pidFileName string) error {
	if len(pidFileName) == 0 {
		return errors.New("pid file [" + pidFileName + "] invalid")
	}

	pidFile, err := os.OpenFile(pidFileName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.New("open pid file [" + pidFileName + "] fail")
	}

	if err := syscall.Flock(int(pidFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("lock pid file [" + pidFileName + "] fail")
	}

	pidFile.Truncate(0)
	pidFile.Write([]byte(strconv.Itoa(os.Getpid())))
	return nil
}
