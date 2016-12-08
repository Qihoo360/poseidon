package job

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"common"
	ds "poseidon/datastruct"

	sj "github.com/bitly/go-simplejson"
	"github.com/donnie4w/go-logger/logger"
	"github.com/golang/protobuf/proto"
)

const TimeFormatMinute = "2006-01-02-15-04"
const TimeFormatHour = "2006-01-02-15"

type StrSlice []string
type LogInfo struct {
	logs      StrSlice
	urls      StrSlice
	hourTime  string
	totalSize int64
	startTime time.Time
}
type DocIdInfo struct {
	Count      int    `json:"count"`
	Errmsg     string `json:"errmsg"`
	Errno      int    `json:"errno"`
	StartIndex int    `json:"start_index"`
	Time       int    `json:"time"`
}
type SLMap map[string] /*idc*/ LogInfo

type LogtoHdfsCollector struct {
	sub           string
	id            int
	shortHostname string

	flushMinute  int // if now - t > flushMinute; then flush
	gatherMinute int // gather logs, combined every 5 minutes

	tickInterval  int       // 并不是每次tick都flush，tickInterval * tick 才flush一次
	currentTick   int       // 配合tickInterval使用
	lastFlushTime time.Time // 上次flush的时间，防止由于过多的flushDueSize导致长时间不能flush

	maxMergeFileByte int64

	writeDirs                       []string
	hadoopRemoteDir                 string
	hadoopRemoteTimeDirs            []string
	hadoopRemoteTimeDirTruncMinutes []int
	hadoopRemoteFilePrefix          string
	hadoopRemoteFileSuffix          string

	hadoopCmd string

	needZip   bool
	needUnzip bool

	loc    *time.Location
	logMap map[time.Time]SLMap

	poseidonMode    bool // 只支持gz,不支持明文
	docLines        int
	readBufSizeByte int
	docIdDomain     string
	docIdBusiness   string
}

func (p *LogtoHdfsCollector) Init(ctx *sj.Json, id int) error {
	p.sub = ctx.Get("runtime").Get("sub").MustString("NIL")
	p.id = id

	var err error
	p.loc, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return err
	}

	p.writeDirs = ctx.Get("Hdfs").Get("write_dirs").MustStringArray([]string{"/data1/ck/logto_hdfs"})
	p.hadoopRemoteDir = ctx.Get("Hdfs").Get("hadoop_remote_dir").MustString("")
	p.hadoopRemoteFilePrefix = ctx.Get("Hdfs").Get("hadoop_remote_file_prefix").MustString("")
	p.hadoopRemoteFileSuffix = ctx.Get("Hdfs").Get("hadoop_remote_file_suffix").MustString("")
	p.hadoopRemoteTimeDirs = ctx.Get("Hdfs").Get("hadoop_remote_time_dirs").MustStringArray()
	p.hadoopRemoteTimeDirTruncMinutes = make([]int, len(p.hadoopRemoteTimeDirs))
	for i, timeDir := range p.hadoopRemoteTimeDirs {
		if len(timeDir) <= 0 {
			return errors.New("hadoop_remote_time_dir must not be empty!")
		}
		p.hadoopRemoteTimeDirs[i], p.hadoopRemoteTimeDirTruncMinutes[i], err = p.transferTimeFormat(timeDir)
		if err != nil {
			return err
		}
	}
	if len(p.hadoopRemoteTimeDirs) <= 0 {
		return errors.New("there is no hadoop_remote_time_dirs!")
	}

	p.hadoopCmd = ctx.Get("Hdfs").Get("hadoop_cmd").MustString("/usr/bin/hadoop/software/hadoop/bin/hadoop")

	p.flushMinute = ctx.Get("Hdfs").Get("flush_minute").MustInt(10)
	p.gatherMinute = ctx.Get("Hdfs").Get("gather_minute").MustInt(5)
	p.tickInterval = ctx.Get("Hdfs").Get("tick_interval").MustInt(10)

	p.lastFlushTime = time.Now()

	p.maxMergeFileByte = ctx.Get("Hdfs").Get("max_merge_file_size").MustInt64(268435456) // 256M. the block size of HDFS currently

	for _, v := range p.writeDirs {
		if err := os.MkdirAll(v, 0777); err != nil {
			logger.ErrorSubf(p.sub, "writeDir err: %v, dir: %s", err, v)
			return errors.New("mkdir failed " + v)
		}
	}

	if _, err := os.Stat(p.hadoopCmd); err != nil && !os.IsExist(err) {
		logger.ErrorSub(p.sub, "hadoopCmd err:", err)
		return err
	}

	// between 1~60, like 2, 4, 5, 10, 15, 20, 30
	if p.gatherMinute <= 0 || p.gatherMinute >= 60 || 60%p.gatherMinute != 0 {
		return errors.New("gatherMinute invalid")
	}
	if p.flushMinute < p.gatherMinute {
		return errors.New("flushMinute invalid")
	}
	if p.maxMergeFileByte < 1048576 || p.maxMergeFileByte > 10737418240 {
		return errors.New("maxMergeFileByte should be 1048576-1073741824") // 1M~10G
	}

	p.logMap = make(map[time.Time]SLMap)

	host, err := os.Hostname()
	if err != nil {
		return err
	}
	strArr := strings.Split(host, ".")
	p.shortHostname = strArr[0]

	p.needZip = ctx.Get("Hdfs").Get("need_zip").MustBool(false)
	p.needUnzip = ctx.Get("Hdfs").Get("need_unzip").MustBool(false)
	if p.needZip && p.needUnzip { // 不能同时为true
		p.needZip = false
		p.needUnzip = false
	}

	p.docLines = ctx.Get("Hdfs").Get("doc_lines").MustInt(128)
	p.poseidonMode = ctx.Get("Hdfs").Get("poseidon_mode").MustBool(false)
	p.readBufSizeByte = ctx.Get("Hdfs").Get("readbuf_size_byte").MustInt(1024 * 1024)
	p.docIdDomain = ctx.Get("Hdfs").Get("docid_domain").MustString("127.0.0.1:39360")
	p.docIdBusiness = ctx.Get("Hdfs").Get("docid_business").MustString("temp")

	return nil
}

func (p *LogtoHdfsCollector) Collect(item Item) error {
	//	logger.DebugSub(p.sub, "collector get: ", item)
	id := item.Id
	var minute time.Time
	var fileInfo os.FileInfo
	var fileSize int64
	var tmpLogInfo LogInfo

	// 去重
	hour, err := common.GetHourStrFromId(id)
	if err != nil {
		logger.ErrorSub(p.sub, "id "+id+"not contain valid timestr", err)
		hour = ""
	}

	var path string
	idc := item.Content
	minutePtr, err := p.getMinute(item.RawMsg)
	if err != nil {
		goto ERROR
	}

	// zip or unzip
	// 这里是预处理，poseidon模式需要输入数据是压缩的，可以在这里提前弄一下
	// 如果zip和unzip都不需要，这一步什么都不做
	item.RawMsg, err = p.HandleZip(item.RawMsg)
	if err != nil {
		goto ERROR
	}
	path = item.RawMsg
	minute = *minutePtr
	fileInfo, err = os.Stat(path)
	if err != nil {
		goto ERROR
	}
	fileSize = fileInfo.Size()
	if fileSize <= 0 {
		goto FINISH
	}

	if _, ok := p.logMap[minute]; !ok {
		p.logMap[minute] = make(SLMap)
	}
	if _, ok := p.logMap[minute][idc]; !ok {
		p.logMap[minute][idc] = LogInfo{startTime: time.Now(), hourTime: hour}
	}

	tmpLogInfo = p.logMap[minute][idc]
	tmpLogInfo.logs = append(tmpLogInfo.logs, path)
	tmpLogInfo.urls = append(tmpLogInfo.urls, id)
	tmpLogInfo.totalSize += fileSize
	p.logMap[minute][idc] = tmpLogInfo

	if tmpLogInfo.totalSize >= p.maxMergeFileByte {

		curTime := time.Now()
		if curTime.Sub(tmpLogInfo.startTime) < time.Duration(1)*time.Second {
			// sleep for 1 sec, in case file name conflict.
			// merge后的文件名,时间精确到秒,因此不能在同一秒生成多个merge文件
			time.Sleep(time.Duration(1) * time.Second)
		}
		if err := p.copyLogToHdfs(minute, idc, tmpLogInfo.logs, tmpLogInfo.urls); err != nil {
			logger.ErrorSub(p.sub, p.id, "copyLogToHdfs fail with reason [", err, ", [", tmpLogInfo, "]")
		}

		logger.DebugSub(p.sub, p.id, "copyLogToHdfs due to size")
		p.logMap[minute][idc] = LogInfo{startTime: curTime, hourTime: hour}
	}

FINISH:
	logger.DebugSub(p.sub, "collect finish", p.id, item)
	return nil
ERROR:
	logger.ErrorSub(p.sub, "collect fail", p.id, item, err)
	return err
}

func (p *LogtoHdfsCollector) Destory() error {
	logger.DebugSub(p.sub, p.id, "collector destroy")
	return p.flush(true)
}

func (p *LogtoHdfsCollector) Tick() error {
	p.currentTick++
	if p.currentTick > p.tickInterval {
		p.currentTick = 0
		logger.DebugSubf(p.sub, "LogtoHdfsCollector.Tick %d this is a flush tick", p.id)
		return p.flush(false)
	}
	logger.DebugSubf(p.sub, "LogtoHdfsCollector.Tick id: %d no flush tick: %d", p.id, p.currentTick)

	now := time.Now()
	if now.Sub(p.lastFlushTime).Minutes() > 10 {
		logger.InfoSubf(p.sub, "LogtoHdfsCollector.Tick id: %d no flush tick: %d, but too long ago last flush: %v",
			p.id, p.currentTick, p.lastFlushTime)
		p.lastFlushTime = now
		return p.flush(false)
	}
	return nil
}

func (p *LogtoHdfsCollector) getMinute(path string) (*time.Time, error) {

	// new format:   info.2016-01-08-13.gz
	// new format:   info.2016-01-08-13-01.gz
	// 支持2种格式,分别精确到分钟和小时
	// 返回值是Truncate过后的值，因此同一个gatherMinute区间的文件会归并到一块

	var t time.Time
	curYear := strconv.Itoa(time.Now().Year())
	idx := strings.Index(path, curYear+"-")
	if idx < 0 {
		return nil, errors.New("path not contain yearStr")
	}

	subStr := path[idx:] // 2016-01-08-13-01.gz or 2016-01-08-13.gz

	if len(subStr) >= len(TimeFormatMinute) {
		minute, err := time.ParseInLocation(TimeFormatMinute, subStr[0:len(TimeFormatMinute)], p.loc) // get "2015-12-14-17-00"
		if err == nil {
			t = minute.Truncate(time.Duration(p.gatherMinute) * time.Minute)
			return &t, nil
		}
	}
	if len(subStr) >= len(TimeFormatHour) {
		minute, err := time.ParseInLocation(TimeFormatHour, subStr[0:len(TimeFormatHour)], p.loc) // get "2015-12-14-17"
		if err == nil {
			t = minute.Truncate(time.Duration(p.gatherMinute) * time.Minute)
			return &t, nil
		}
	}
	return nil, errors.New("path timeformat error")
}

func (p *LogtoHdfsCollector) HandleZip(path string) (string, error) {
	var newPath string
	var cmdArr []string
	isGz := strings.HasSuffix(path, ".gz")
	if p.needUnzip && isGz {
		cmdArr = append(cmdArr, "-d")
		newPath = path[0 : len(path)-3]
	} else if p.needZip && !isGz {
		newPath = path + ".gz"
	} else {
		return path, nil
	}

	cmdArr = append(cmdArr, path)
	c := exec.Command("/bin/gzip", cmdArr...)
	if c == nil {
		return "", errors.New("new zip/unzip cmd fail " + path)
	}
	err := c.Run()
	return newPath, err
}

type TimeSlice []time.Time

func (p TimeSlice) Len() int           { return len(p) }
func (p TimeSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p TimeSlice) Less(i, j int) bool { return p[i].Before(p[j]) }

func (p *LogtoHdfsCollector) flush(flushAll bool) error {
	logger.DebugSubf(p.sub, "LogtoHdfsCollector.flush %d, begin flush flushAll: %v", p.id, flushAll)

	timeBegin := time.Now()
	defer func() {
		timeCost := (time.Since(timeBegin).Nanoseconds() / 1000000)
		logger.DebugSubf(p.sub, "LogtoHdfsCollector.flush %d, flushAll: %v, cost: %d", p.id, flushAll, timeCost)
	}()

	timeThreshold := time.Duration(p.flushMinute) * time.Minute

	// 最早的最先处理
	times := []time.Time{}
	for t, _ := range p.logMap {
		times = append(times, t)
	}
	sort.Sort(TimeSlice(times))

	for _, t := range times {
		slMap := p.logMap[t]
		logger.DebugSubf(p.sub, "LogtoHdfsCollector.flush %d len(logMap): %d", p.id, len(p.logMap))
		if len(slMap) <= 0 {
			delete(p.logMap, t)
			continue
		}
		curTime := time.Now()
		if !flushAll && curTime.Sub(t) < timeThreshold {
			continue
		}

		for idc, logInfo := range slMap {
			vec := logInfo.logs
			if len(vec) <= 0 {
				delete(p.logMap[t], idc)
				continue
			}
			if time.Since(logInfo.startTime) < time.Duration(1)*time.Second {
				// sleep for 1 sec, in case file nanme conflict
				time.Sleep(time.Duration(1) * time.Second)
			}
			if err := p.copyLogToHdfs(t, idc, vec, logInfo.urls); err != nil {
				logger.ErrorSub(p.sub, p.id, "copyLogToHdfs fail with reason [", err, "], [", logInfo, "]")
			}

			delete(p.logMap[t], idc)
		}
	}

	if flushAll {
		logger.InfoSub(p.sub, p.id, "flush_when_exit: [", p.logMap, "]")
	}
	logger.DebugSubf(p.sub, "LogtoHdfsCollector.flush %d, flush end flushAll: %v", p.id, flushAll)
	return nil
}

// 将文件列表files组装好入hdfs，组装可能是poseidon模式
// 这时t是logMap的key，是truncate后的时间，比如2016-06-06-12-15，一个t对应的全部文件都入到hadoop中一个文件中
func (p *LogtoHdfsCollector) copyLogToHdfs(t time.Time, idc string, files []string, urls []string) error {
	timeBegin := time.Now()
	defer func() {
		timeCost := (time.Since(timeBegin).Nanoseconds() / 1000000)
		logger.DebugSubf(p.sub, "LogtoHdfsCollector.copyLogToHdfs %d, len(files): %d, cost: %d", p.id, len(files), timeCost)
	}()

	if len(files) <= 0 {
		return nil
	}
	var err error
	var localGzFile string
	var localDocidGzFile string
	var localGzFileSize int64

	timeStr := t.Format(TimeFormatMinute)
	subPath, compressRemotePath := p.getRemoteSubPath(t, timeStr)

	logger.DebugSubf(p.sub, "LogtoHdfsCollector.copyLogToHdfs %d begin, len(files): %d, subPath: %s", p.id, len(files), subPath)
	if p.poseidonMode {
		localGzFile, localDocidGzFile, err = p.generateNewGzAndMetaFile(timeStr, idc, files, compressRemotePath)
		if err != nil {
			logger.ErrorSub(p.sub, p.id, "generate fail,", localGzFile, localDocidGzFile)
			return err
		}
	} else {
		localGzFile, err = p.generateNewGzFile(timeStr, idc, files)
		if err != nil {
			logger.ErrorSub(p.sub, p.id, "generate fail,", localGzFile)
			return err
		}
	}
	if err != nil {
		return err
	}

	// 处理文件大小为0的情况
	localGzFileSize, err = p.getSize(localGzFile)
	if err != nil {
		return err
	}
	if localGzFileSize <= 0 {
		bakFileName := filepath.Dir(localGzFile) + "/bak." + filepath.Base(localGzFile)
		if err := os.Rename(localGzFile, bakFileName); err != nil {
			return err
		}
		logger.InfoSub(p.sub, bakFileName, "size = 0, ignore")
		return nil
	}

	if p.poseidonMode {
		if err = p.copySingleFileToHdfs(localDocidGzFile, p.hadoopRemoteDir+"/docid/"+subPath); err != nil {
			logger.ErrorSub(p.sub, p.id, "copy", localDocidGzFile, "=>", p.hadoopRemoteDir+"/docid/"+subPath, "fail")
			return err
		}
	}

	// 执行拷贝
	err = p.copySingleFileToHdfs(localGzFile, p.hadoopRemoteDir+"/"+subPath)
	if err != nil {
		logger.ErrorSub(p.sub, p.id, "copy", localGzFile, "=>", p.hadoopRemoteDir+"/"+subPath, "fail")
		return err
	}

	logger.InfoSubf(p.sub, "%dth copy localGzFile[%s] (contains files{%v}) (contains urls{%v}) to hdfs[%s] success\n", p.id, localGzFile, files, urls, subPath)

	return nil
}

func (p *LogtoHdfsCollector) getSize(path string) (int64, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	fileSize := fileInfo.Size()
	return fileSize, nil
}

func (p *LogtoHdfsCollector) getRemoteSubPath(t time.Time, timeStr string) (string, string) {
	dir := ""
	for i := 0; i < len(p.hadoopRemoteTimeDirs); i++ {
		timeDir := p.hadoopRemoteTimeDirs[i]
		if p.hadoopRemoteTimeDirTruncMinutes[i] > 1 {
			// 为了满足以下需求：
			// 配置为 "Hi:15",期望的行为是,生成的子目录为 "小时+分钟（按每15分钟一个目录）"
			// 即目录为1200/1215/1230/1245,目录下的文件按分钟来入
			tmpT := t.Truncate(time.Duration(p.hadoopRemoteTimeDirTruncMinutes[i]) * time.Minute)
			dir += tmpT.Format(timeDir) + "/"
		} else {
			dir += t.Format(timeDir) + "/"
		}
	}
	curTimeStr := time.Now().Format("0102150405")
	strId := strconv.Itoa(p.id)
	subPath := dir + p.hadoopRemoteFilePrefix + strId + "_" + p.shortHostname + "_" + curTimeStr + "_" + timeStr + p.hadoopRemoteFileSuffix

	compressPath := strId + "_" + p.shortHostname + "_" + curTimeStr + timeStr[11:13] + timeStr[14:]
	return subPath, compressPath
}

func (p *LogtoHdfsCollector) generateNewGzFile(timeStr string, idc string, files []string) (string, error) {
	writeDir := p.writeDirs[rand.Intn(len(p.writeDirs))]
	dir := writeDir + "/" + timeStr[0:10] + "/" + timeStr[11:13] // timeStr is like 2015-12-15-12-25; get YYYY-mm-dd and HH
	err := os.MkdirAll(dir, 0777)                                // like mkdir -p
	if err != nil {
		return "", err
	}

	var cmdArr []string
	cmdArr = append(cmdArr, files...)
	cmdArr = append(cmdArr)
	cmd := exec.Command("/bin/cat", cmdArr...)

	curTimeStr := time.Now().Format("20060102150405")
	strId := strconv.Itoa(p.id)
	newGzFileName := dir + "/" + strId + "_" + idc + "_" + curTimeStr + "_access.log." + timeStr + ".gz"
	outfile, err := os.Create(newGzFileName)
	if err != nil {
		return "", err
	}
	defer outfile.Close()

	cmd.Stdout = outfile // redirect the output to file
	err = cmd.Start()
	if err != nil {
		return "", err
	}
	err = cmd.Wait()

	return newGzFileName, nil
}

func (p *LogtoHdfsCollector) generateNewGzAndMetaFile(timeStr string, idc string, inputFiles []string, compressRemotePath string) (newGzFileName string, newMetaFileName string, err error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	timeBegin := time.Now()
	defer func() {
		costTime := time.Since(timeBegin)

		var fileSize int64
		var speed int
		if fileInfo, err := os.Stat(newGzFileName); err == nil {
			if fileSize = fileInfo.Size(); fileSize > 0 {
				speed = int((float64(fileSize) / costTime.Minutes()) / 1024 / 1024)
			}
		}

		logger.DebugSubf(p.sub, "LogtoHdfsCollector.generateNewGzAndMetaFile %d, len(inputFiles): %d, cost: %v, gz: %s, docid: %s, size: %d M, speed: %d M/minute",
			p.id, len(inputFiles), costTime, newGzFileName, newMetaFileName, fileSize/1024/1024, speed)
	}()

	// 平衡多块磁盘的负载
	writeDir := p.writeDirs[rand.Intn(len(p.writeDirs))]
	dir := writeDir + "/" + timeStr[0:10] + "/" + timeStr[11:13] // timeStr is like 2015-12-15-12-25; get YYYY-mm-dd and HH
	err = os.MkdirAll(dir, 0777)                                 // like mkdir -p
	if err != nil {
		return "", "", err
	}

	curTimeStr := time.Now().Format("20060102-150405")
	strId := strconv.Itoa(p.id)
	newGzFileName = dir + "/" + strId + "_" + idc + "_" + curTimeStr + "_access.log." + timeStr + ".gz"
	newMetaFileName = dir + "/" + strId + "_" + idc + "_" + curTimeStr + "_access.docid." + timeStr + ".gz"

	f, err := os.OpenFile(newGzFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	var docStr string
	var docLineCount int
	var docSlice []ds.DocGzMeta
	for _, inputFile := range inputFiles {
		inf, err := os.Open(inputFile)
		if err != nil {
			// return "", "", errors.New("open gz file fail: " + inputFile)
			logger.ErrorSubf(p.sub, "LogtoHdfsCollector.generateNewGzAndMetaFile open gz file %s fail: %v",
				inputFile, err)
			continue
		}
		defer inf.Close()

		reader := bufio.NewReaderSize(inf, p.readBufSizeByte)

		if strings.HasSuffix(inputFile, ".gz") {
			gz, err := gzip.NewReader(inf)
			if err != nil {
				// return "", "", errors.New("init gzip fail: " + inputFile)
				logger.ErrorSubf(p.sub, "LogtoHdfsCollector.generateNewGzAndMetaFile init gzip fail: %v file: %s",
					err, inputFile)
				continue
			}
			defer gz.Close()

			reader = bufio.NewReaderSize(gz, p.readBufSizeByte)
		}

		var line string
		var read_err error
		for true {
			line, read_err = reader.ReadString('\n')
			if read_err != nil && read_err != io.EOF {
				// return "", "", read_err
				logger.ErrorSubf(p.sub, "LogtoHdfsCollector.generateNewGzAndMetaFile read gz fail: %v file: %s",
					read_err, inputFile)
				break
			}
			if len(line) <= 0 {
				goto CHECK_STATUS
			}

			docLineCount++
			docStr += line
			if docLineCount >= p.docLines {
				// write do gz
				offset, size, err := p.writeGzFile(f, docStr)
				if err != nil {
					logger.ErrorSubf(p.sub, "LogtoHdfsCollector.generateNewGzAndMetaFile write gz fail: %v file: %s",
						err, inputFile)
					return "", "", err
				}
				docSlice = append(docSlice, ds.DocGzMeta{
					Path:   compressRemotePath,
					Offset: uint32(offset),
					Length: uint32(size),
				})

				docStr = ""
				docLineCount = 0
			}

		CHECK_STATUS:
			if read_err == io.EOF {
				break
			}
		}
	}

	if docLineCount > 0 {
		offset, size, err := p.writeGzFile(f, docStr)
		if err != nil {
			logger.ErrorSubf(p.sub, "LogtoHdfsCollector.generateNewGzAndMetaFile write gz fail: %v", err)
			return "", "", err
		}
		docSlice = append(docSlice, ds.DocGzMeta{
			Path:   compressRemotePath,
			Offset: uint32(offset),
			Length: uint32(size),
		})
	}

	if err = p.writeMetaFile(docSlice, timeStr, newMetaFileName); err != nil {
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.generateNewGzAndMetaFile write meta file fail: %v", err)
		return "", "", err
	}

	return newGzFileName, newMetaFileName, nil
}

func (p *LogtoHdfsCollector) writeMetaFile(docSlice []ds.DocGzMeta, timeStr string, newMetaFileName string) error {
	count := len(docSlice)
	if count <= 0 {
		return nil
	}
	timeSlice := strings.Split(timeStr, "-")
	fmt.Print(timeSlice)
	if len(timeSlice) != 5 || len(timeSlice[0]) != 4 || len(timeSlice[1]) != 2 || len(timeSlice[2]) != 2 {
		return errors.New("timeSlice error")
	}
	url := fmt.Sprintf("http://%s/service/idgenerator?count=%d&business_name=%s&day=%s", p.docIdDomain, count, p.docIdBusiness, timeSlice[0]+timeSlice[1]+timeSlice[2])
	logger.DebugSubf(p.sub, "LogtoHdfsCollector.writeMetaFile url: %v", url)
	resp, err := http.Get(url)
	if err != nil {
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.writeMetaFile http.Get fail url: %s", url)
		return err
	}
	var docIdInfo DocIdInfo
	ret, _ := ioutil.ReadAll(resp.Body)
	if err = json.Unmarshal(ret, &docIdInfo); err != nil {
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.writeMetaFile json.Unmarshal err ret: %v, url: %v",
			string(ret), url)
		return err
	}
	if docIdInfo.Errno != 0 {
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.writeMetaFile bad docIdInfo.Errno: %v", docIdInfo.Errno)
		return errors.New(docIdInfo.Errmsg)
	}
	if docIdInfo.Count != count {
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.writeMetaFile bad docIdInfo.Count: %d", docIdInfo.Count)
		return errors.New("cannot get enough docid")
	}
	startId := docIdInfo.StartIndex
	content := ""
	for i := 0; i < count; i++ {
		content += strconv.Itoa(startId+i) + "\t"
		data, err := proto.Marshal(&docSlice[i])
		if err != nil {
			logger.ErrorSubf(p.sub, "LogtoHdfsCollector.writeMetaFile proto.Marshal err: %v", docSlice[i])
			return err
		}
		content += base64.StdEncoding.EncodeToString([]byte(data)) + "\n"
	}
	f, err := os.OpenFile(newMetaFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.writeMetaFile os.OpenFile err, filename: %s", newMetaFileName)
		return err
	}
	defer f.Close()

	if _, _, err = p.writeGzFile(f, content); err != nil {
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.writeMetaFile p.writeGzFile err: %v", err)
		return err
	}
	return nil
}

func (p *LogtoHdfsCollector) writeGzFile(f *os.File, s string) (int, int, error) {
	var err error
	var j int
	var offset int64

	if offset, err = f.Seek(0, 1); err != nil {
		return 0, 0, err
	}
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if w == nil {
		return 0, 0, errors.New("gzip.NewWriter fail")
	}

	if _, err = w.Write([]byte(s)); err != nil {
		return 0, 0, err
	}
	w.Close()

	if j, err = f.Write(b.Bytes()); err != nil {
		return 0, 0, err
	}
	return int(offset), j, nil
}

func (p *LogtoHdfsCollector) copySingleFileToHdfs(localPath string, remotePath string) error {
	// 不成功就一直重试，这样hadoop客户端出问题就会停止不前，以免丢数据
	retry := 0
	var err error
	// makedir -p
	for true {
		remoteDir := filepath.Dir(remotePath)
		cmdArr := []string{p.hadoopCmd, "fs", "-mkdir", "-p", remoteDir}
		c := exec.Command("bash", cmdArr...)
		if c == nil {
			return errors.New("new cmd fail")
		}
		// logger.DebugSubf(p.sub, "LogtoHdfsCollector.copySingleFileToHdfs mkdir, remoteDir: %s, cmd: %v", remoteDir, c)
		err = c.Run()
		if err == nil {
			logger.InfoSub(p.sub, p.id, "mkdir", remoteDir, "success")
			break
		}
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.copySingleFileToHdfs mkdir err, remoteDir: %s, remotePath: %v, retry: %d, err: %v",
			remoteDir, remotePath, retry, err)
		retry++
		time.Sleep(time.Second * time.Duration(3))
	}
	for true {
		t1 := time.Now()
		cmdArr := []string{p.hadoopCmd, "fs", "-copyFromLocal", localPath, remotePath}
		c := exec.Command("bash", cmdArr...)
		if c == nil {
			return errors.New("new cmd fail")
		}
		err = c.Run()
		//fmt.Printf("cmd: [%+v]\n", c)
		if err == nil {
			bakFileName := filepath.Dir(localPath) + "/bak." + filepath.Base(localPath)
			if err := os.Rename(localPath, bakFileName); err != nil {
				return err
			}
			t2 := time.Since(t1)
			logger.InfoSub(p.sub, p.id, "copy", bakFileName, "to", remotePath, "cost:", t2, ", retry:", retry)
			return nil
		}
		logger.ErrorSubf(p.sub, "LogtoHdfsCollector.copySingleFileToHdfs err, localPath: %v, remotePath: %v, retry: %d, err: %v",
			localPath, remotePath, retry, err)
		retry++
		time.Sleep(time.Second * time.Duration(3))
	}
	badFileName := filepath.Dir(localPath) + "/bad." + filepath.Base(localPath)
	os.Rename(localPath, badFileName)
	logger.ErrorSubf(p.sub, "LogtoHdfsCollector.copySingleFileToHdfs failed, err: %v, badFileName: %s", err, badFileName)
	return err
}

func (p *LogtoHdfsCollector) transferTimeFormat(phpStyleFormat string) (string, int, error) {
	var goStyleFormat string
	var truncMinute int
	var err error
	arr := strings.Split(phpStyleFormat, ":")
	if len(arr) >= 2 {
		truncMinute, err = strconv.Atoi(arr[1])
		if err != nil {
			return "", 0, err
		}
	}
	goStyleFormat = strings.Replace(arr[0], "Y", "2006", -1)
	goStyleFormat = strings.Replace(goStyleFormat, "m", "01", -1)
	goStyleFormat = strings.Replace(goStyleFormat, "d", "02", -1)
	goStyleFormat = strings.Replace(goStyleFormat, "H", "15", -1)
	goStyleFormat = strings.Replace(goStyleFormat, "i", "04", -1)
	goStyleFormat = strings.Replace(goStyleFormat, "s", "05", -1)
	return goStyleFormat, truncMinute, nil
}
