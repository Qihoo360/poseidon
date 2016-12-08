package main

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"common"
	"time"

	sj "github.com/bitly/go-simplejson"
	"github.com/donnie4w/go-logger/logger"
	//	"github.com/donnie4w/go-logger/logger"
)

type DmSrc struct {
	lastFileMap map[string]string // target monitored dir => lastfilename(save the lastfile that has been provided)

	monitorIntervalSec int
	popLastFile        bool   // whether to ignore the newest file
	dataDir            string // placing the lastfiles
	timeOutMs          int
}

func (p *DmSrc) Init(ctx *sj.Json) error {
	var err error

	p.monitorIntervalSec = ctx.Get("monitor_interval_sec").MustInt(10)
	p.popLastFile = ctx.Get("pop_last_file").MustBool(true)
	p.timeOutMs = ctx.Get("time_out_ms").MustInt(10000)
	p.dataDir = ctx.Get("data_dir").MustString("/tmp")
	stat, err := os.Stat(p.dataDir)
	if (err != nil && !os.IsExist(err)) || !stat.IsDir() {
		return errors.New("data dir not valid")
	}

	p.lastFileMap = make(map[string]string)
	paths := ctx.Get("monitor_paths").MustStringArray()
	if len(paths) <= 0 {
		return errors.New("no monitor_paths is provided")
	}
	if err = p.initMonitorPaths(paths); err != nil {
		return err
	}
	return nil
}

func (p *DmSrc) GetNextMsg() ([][]byte, error) {
	files, err := p.getNextMsg()
	if len(files) <= 0 {
		time.Sleep(time.Duration(p.timeOutMs) * time.Millisecond)
		files, err = p.getNextMsg()
	}
	return files, err
}

func (p *DmSrc) getNextMsg() ([][]byte, error) {
	var files [][]byte
	var err error
	var lastFile string
	var fileList []string
	var newFileList []string

	for path, lastRecordFile := range p.lastFileMap {
		lastFile, err = common.ReadFile(lastRecordFile)
		if err != nil {
			return files, err
		}
		lastFile = strings.TrimSpace(lastFile)

		fileList, err = p.getFileList(path)
		if err != nil {
			return files, err
		}

		if newFileList, err = p.getNewFile(lastFile, fileList); err != nil {
			return files, err
		}

		if len(newFileList) <= 0 {
			continue
		}
		if p.popLastFile {
			newFileList = newFileList[0 : len(newFileList)-1]
			if len(newFileList) <= 0 {
				continue
			}
		}

		// write last file
		for _, newFile := range newFileList {
			err = common.WriteFile(lastRecordFile, newFile)
			if err != nil {
				return files, err
			}
			files = append(files, []byte(path+"/"+newFile))
		}
	}
	return files, nil
}

func (p *DmSrc) Ack() error {
	return nil
}

func (p *DmSrc) Destory() error {
	return nil
}

func (p *DmSrc) initMonitorPaths(paths []string) error {

	curDir, err := os.Getwd()
	if err != nil {
		return err
	}
	inodeMap := make(map[uint64]int)
	for _, path := range paths {
		logger.Info("DmSrc.initMonitorPaths path:", path)
		path = strings.TrimRight(path, "/")
		if !strings.HasPrefix(path, "/") { // in case it is a relative dir
			path = curDir + "/" + path
		}

		// remove the duplicated dirs according to inode
		var stat syscall.Stat_t
		if err = syscall.Stat(path, &stat); err != nil {
			return err
		}
		if _, ok := inodeMap[stat.Ino]; ok {
			continue
		}

		fi, err := os.Stat(path)
		if (err != nil && !os.IsExist(err)) || !fi.IsDir() {
			return errors.New(path + " data dir not valid")
		}
		p.lastFileMap[path] = p.getLastRecordFileName(path)
		_, err = os.Stat(p.lastFileMap[path])
		if err != nil && !os.IsExist(err) {
			if err := common.WriteFile(p.lastFileMap[path], "start"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *DmSrc) getNewFile(lastFile string, fileList []string) ([]string, error) {
	if lastFile == "" || lastFile == "start" {
		return fileList, nil
	}

	if len(fileList) <= 0 {
		return fileList, nil
	}

	index := sort.Search(len(fileList), func(i int) bool { return fileList[i] >= lastFile })
	if index >= len(fileList) || fileList[index] < lastFile {
		return []string{}, errors.New(lastFile + " is newer than everyone")
	}
	if fileList[index] == lastFile {
		return fileList[index+1:], nil
	} else {
		return fileList, nil
	}
}

func (p *DmSrc) getLastRecordFileName(path string) string {
	newPath := strings.Replace(path, "/", "-", -1)
	return p.dataDir + "/last" + newPath
}

func (p *DmSrc) getFileList(path string) ([]string, error) {
	var err error
	var f *os.File
	var fileNameArr []string

	if f, err = os.Open(path); err != nil {
		return fileNameArr, err
	}
	defer f.Close()

	fileInfoArr, err := f.Readdir(-1)
	if err != nil {
		return fileNameArr, err
	}
	for _, fileInfo := range fileInfoArr {
		if fileInfo.Name() == "." || fileInfo.Name() == ".." {
			continue
		}
		if fileInfo.IsDir() || strings.TrimSpace(fileInfo.Name()) == "" {
			continue
		}
		fileNameArr = append(fileNameArr, filepath.Base(fileInfo.Name()))
	}
	sort.Strings(fileNameArr)
	return fileNameArr, nil
}
