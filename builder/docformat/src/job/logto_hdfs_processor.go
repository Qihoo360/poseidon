package job

import (
	"errors"
	"os"
	"strings"
	"time"

	sj "github.com/bitly/go-simplejson"
	"github.com/donnie4w/go-logger/logger"
)

// receive local paths, and logto hadoop
type LogtoHdfsProcessor struct {
	sub         string
	id          int
	itemChans   [](chan Item)
	fixedIdc    string
	hostName    string
	useFixedIdc bool
}

func (p *LogtoHdfsProcessor) Init(ctx *sj.Json, id int, itemChans [](chan Item)) error {
	p.sub = ctx.Get("runtime").Get("sub").MustString("NIL")
	p.id = id
	p.itemChans = itemChans

	p.fixedIdc = ctx.Get("Hdfs").Get("fixed_idc").MustString("idc")
	p.useFixedIdc = ctx.Get("Hdfs").Get("use_fixed_idc").MustBool(false)

	var err error
	p.hostName, err = GetShortHostName()
	if err != nil {
		return err
	}

	return nil
}

func GetShortHostName() (string, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return "", err
	}
	strArr := strings.Split(hostName, ".")
	if len(strArr) <= 0 || len(strArr[0]) <= 0 {
		return "", errors.New("hostname invalid: " + hostName)
	}
	return strArr[0], nil
}

func (p *LogtoHdfsProcessor) Process(msg string) error {
	logger.DebugSub(p.sub, "LogtoHdfsProcessor.Process begin, get :[", msg, "]")

	msgArr := strings.Split(msg, "\n")
	if len(msgArr) <= 0 {
		return nil
	}
	items := p.getItems(msgArr)
	for _, item := range items {
		if _, err := os.Stat(item.RawMsg); err != nil && !os.IsExist(err) {
			logger.ErrorSub(p.sub, err)
			continue
		}

		// processor不要堵塞，当有空闲collector时要及时发送
	LOOP:
		for {
			var index = GetLeastBusyChannel(p.itemChans)
			logger.DebugSub(p.sub, "LogtoHdfsProcessor.Process index:", index, "path:", item.RawMsg)
			select {
			case p.itemChans[index] <- item:
				break LOOP
			default:
				time.Sleep(5 * time.Second)
			}

		}
	}

	return nil
}

func (p *LogtoHdfsProcessor) Tick() error {
	return nil
}

func (p *LogtoHdfsProcessor) Destory() error {
	logger.DebugSub(p.sub, "LogtoHdfsProcessor destroy ", p.id)
	return nil
}

func (p *LogtoHdfsProcessor) getItems(msgs []string) []Item {
	var items []Item
	for _, msg := range msgs {
		if len(msg) <= 0 {
			continue
		}
		strArr := strings.Split(msg, "\t")
		if len(strArr) != 3 && len(strArr) != 1 {
			logger.ErrorSub(p.sub, "msg invalid, split error ", msg)
			continue
		}
		var path, id string

		if len(strArr) == 3 {
			// msg format: id \t host \t path
			id = strings.TrimSpace(strArr[0])
			path = strings.TrimSpace(strArr[2])
		} else if len(strArr) == 1 { // from dm_src_provider.
			// msg format: path
			path = strings.TrimSpace(msg)
			id = p.hostName + ":" + path
		}

		if len(path) < 20 {
			logger.ErrorSub(p.sub, "msg invalid, path error ", msg)
			continue
		}
		item := Item{
			Category: "logto_hdfs",
			Id:       id,
			Content:  p.fixedIdc,
			RawMsg:   path,
		}
		items = append(items, item)
	}
	return items
}
