package main

import (
	"errors"
	"time"

	sj "github.com/bitly/go-simplejson"
	"github.com/donnie4w/go-logger/logger"
	"job"
)

type Workshop struct {
	sub           string // 业务的名字，打日志时有用
	ctx           *sj.Json
	sleepWhenNeed bool // 当机器不健康时消极怠工

	ctrlChan   chan int // [0] unis -> channel -> control
	reportChan chan int // [0] control -> channel -> unis

	processorNum int
	collectorNum int

	workCtrlChan   chan int // [1] control -> channel -> work
	workReportChan chan int // [0] work -> channel -> control

	msgChan   chan string       // [n] work -> channel -> processor
	itemChans [](chan job.Item) // [n] processor -> channel -> collector

	PRCtrlChans  [](chan int) // [1] control -> channel(try) -> processor || work -> channel(wait) -> processor
	PRReportChan chan int     // [0] processor -> channel -> control
	CLCtrlChans  [](chan int) // [1] control -> channel(try) -> processor || work -> channel(wait) -> processor
	CLReportChan chan int     // [0] collector -> channel -> control

	provider SrcProvider

	// memLimit uint64

	processedLine int

	ticker_ *time.Ticker
}

func (p *Workshop) Init(sub string, ctx *sj.Json, ctrlChan chan int, reportChan chan int) (err error) {
	p.sub = sub

	// 将pub传下去，供processor和collector用
	ctx.Set("runtime", map[string]interface{}{
		"sub": sub,
	})

	p.ctx = ctx
	p.ctrlChan = ctrlChan
	p.reportChan = reportChan

	p.processedLine = 0

	if err = p.initSrcProvider(); err != nil {
		logger.WarnSubf(p.sub, "Workshop.Init err: %v", err)
		return err
	}

	// p.memLimit = ctx.Get("main").Get("mem_limit").MustUint64()

	tickSec := ctx.Get("main").Get("tick_sec").MustInt(5)
	p.ticker_ = time.NewTicker(time.Duration(tickSec) * time.Second)

	p.sleepWhenNeed = ctx.Get("main").Get("sleep_when_need").MustBool(false)

	p.workCtrlChan = make(chan int, 1)
	p.workReportChan = make(chan int)

	p.PRReportChan = make(chan int)
	p.CLReportChan = make(chan int)

	if err = p.initChannel(); err != nil {
		return err
	}

	json_str, _ := p.ctx.MarshalJSON()
	logger.InfoSubf(p.sub, "Workshop.Init success, ctx: %s", string(json_str))

	return nil
}

func (p *Workshop) initChannel() error {
	// create channels
	p.processorNum = p.ctx.Get("main").Get("processor_num").MustInt()
	p.collectorNum = p.ctx.Get("main").Get("collector_num").MustInt()
	msgChanSize := p.ctx.Get("main").Get("msg_chan_size").MustInt()
	itemChanSize := p.ctx.Get("main").Get("item_chan_size").MustInt()

	p.msgChan = make(chan string, msgChanSize)

	p.itemChans = make([](chan job.Item), p.collectorNum)
	for i := 0; i < p.collectorNum; i++ {
		p.itemChans[i] = make(chan job.Item, itemChanSize)
	}

	p.PRCtrlChans = make([](chan int), p.processorNum)
	for i := 0; i < p.processorNum; i++ {
		p.PRCtrlChans[i] = make(chan int, 1)
	}

	p.CLCtrlChans = make([](chan int), p.collectorNum)
	for i := 0; i < p.collectorNum; i++ {
		p.CLCtrlChans[i] = make(chan int, 1)
	}

	// start processor goroutines
	processorSuccessNum := 0
	for i := 0; i < p.processorNum; i++ {
		go p.ProcessRoutine(i, p.PRCtrlChans[i])
	}
	for i := 0; i < p.processorNum; i++ {
		isSuccess := <-p.PRReportChan
		if isSuccess == RET_INIT_SUCCESS {
			processorSuccessNum++
		}
	}
	if processorSuccessNum < p.processorNum {
		logger.WarnSubf(p.sub, "Workshop.initChannel failed processor success %d/%d",
			processorSuccessNum, p.processorNum)
		p.sendCtrlInfo(p.PRCtrlChans, CMD_EXIT)
		return errors.New("some processor init fail")
	}
	logger.InfoSubf(p.sub, "Workshop.initChannel start %d processor, all success", processorSuccessNum)

	// start collector goroutines
	collectorSuccessNum := 0
	for i := 0; i < p.collectorNum; i++ {
		go p.CollectRoutine(i, p.CLCtrlChans[i], p.itemChans[i])
	}
	for i := 0; i < p.collectorNum; i++ {
		isSuccess := <-p.CLReportChan
		if isSuccess == RET_INIT_SUCCESS {
			collectorSuccessNum++
		}
	}
	if collectorSuccessNum < p.collectorNum {
		logger.InfoSubf(p.sub, "Workshop.initChannel failed collector success %d/%d",
			collectorSuccessNum, p.collectorNum)
		p.sendCtrlInfo(p.PRCtrlChans, CMD_EXIT)
		p.sendCtrlInfo(p.CLCtrlChans, CMD_EXIT)
		return errors.New("some colletor init fail")
	}
	logger.InfoSubf(p.sub, "Workshop.initChannel start %d colletors", collectorSuccessNum)

	return nil
}

func (p *Workshop) ProcessRoutine(id int, ctrlChannel chan int) {
	processorFlags := p.ctx.Get("processor").MustMap() // eg: UrlProcessor => true
	logger.InfoSubf(p.sub, "Workshop.ProcessRoutine begin id: %d, processor: %v", id, processorFlags)
	processorMap := make(map[string]job.Processor)
	for name, flag := range processorFlags {
		enable, ok := flag.(bool)
		if !ok {
			logger.ErrorSubf(p.sub, "Workshop.ProcessRoutine processor name: %s, setting invalid flag: %v", name, flag)
			p.PRReportChan <- RET_INIT_FAIL
			return
		}
		if !enable {
			continue
		}
		processorMap[name] = job.NewProcessor(name)
		if processorMap[name] == nil {
			logger.ErrorSubf(p.sub, "Workshop.ProcessRoutine processor %s create failed", name)
			p.PRReportChan <- RET_INIT_FAIL
			return
		}
		if err := processorMap[name].Init(p.ctx, id, p.itemChans); err != nil {
			logger.ErrorSubf(p.sub, "Workshop.ProcessRoutine processor %s init failed, err: %v", name, err)
			delete(processorMap, name)
			p.PRReportChan <- RET_INIT_FAIL
			return
		}
	}

	if len(processorMap) <= 0 {
		logger.ErrorSubf(p.sub, "Workshop.ProcessorRoutine no valid processor")
		p.PRReportChan <- RET_INIT_FAIL
		return
	}

	p.PRReportChan <- RET_INIT_SUCCESS
	logger.InfoSubf(p.sub, "Workshop.ProcessRoutine success id: %d, count: %d", id, len(processorMap))

	exitFlag := false
LOOP:
	for {
		// 退出条件
		if exitFlag && len(p.msgChan) <= 0 {
			logger.InfoSubf(p.sub, "Workshop.ProcessRoutine %d th ProcessRoutine is closing", id)
			break LOOP
		}
		select {
		case cmd := <-ctrlChannel:
			if cmd == CMD_EXIT {
				logger.InfoSubf(p.sub, "Workshop.ProcessRoutine %d th ProcessorRoutine receive exit cmd", id)
				exitFlag = true
			} else if cmd == CMD_TICK {
				for _, proc := range processorMap {
					proc.Tick()
				}
			} else {
				logger.ErrorSubf(p.sub, "Workshop.ProcessRoutine %d th ProcessRoutine receive a un-expected cmd: %v",
					id, cmd)
			}
		case msg := <-p.msgChan:
			for name, proc := range processorMap {
				if err := proc.Process(msg); err != nil {
					logger.ErrorSubf(p.sub, "Workshop.ProcessRoutine %d th ProcessRoutine, %s process fail, err: %v",
						id, name, err)
				}
			}
		case <-time.After(time.Second * 1):
			// logger.Debug(id, "th ProcessRoutine nothing to do")
		} // select
	} // for

	for _, proc := range processorMap {
		proc.Destory()
	}
	p.PRReportChan <- RET_EXIT_SUCCESS
}

func (p *Workshop) CollectRoutine(id int, ctrlChannel chan int, channel chan job.Item) {
	collectorInfos := p.ctx.Get("collector").MustMap()
	logger.InfoSubf(p.sub, "Workshop.CollectRoutine begin id: %d, collector: %v", id, collectorInfos)
	collectorMap := make(map[string]job.Collector)
	for category, name := range collectorInfos {
		collectorName, ok := name.(string)
		if !ok {
			logger.ErrorSubf(p.sub, "Workshop.ProcessRoutine collector setting invalid: %s => %s", category, name)
			p.CLReportChan <- RET_INIT_FAIL
			return
		}
		collectorMap[category] = job.NewCollector(collectorName)
		if collectorMap[category] == nil {
			logger.ErrorSubf(p.sub, "Workshop.ProcessRoutine collector %s create failed", collectorName)
			p.CLReportChan <- RET_INIT_FAIL
			return
		}
		if err := collectorMap[category].Init(p.ctx, id); err != nil {
			logger.ErrorSubf(p.sub, "Workshop.ProcessRoutine collector %s init failed, err: %v", collectorName, err)
			delete(collectorMap, category)
			p.CLReportChan <- RET_INIT_FAIL
			return
		}
	}

	if len(collectorMap) <= 0 {
		logger.ErrorSubf(p.sub, "Workshop.CollectRoutine no valid collector")
		p.CLReportChan <- RET_INIT_FAIL
		return
	}

	p.CLReportChan <- RET_INIT_SUCCESS
	logger.InfoSubf(p.sub, "Workshop.CollectRoutine success id: %d, count: %d", id, len(collectorMap))

	exitFlag := false
LOOP:
	for {
		if exitFlag && len(channel) <= 0 {
			logger.InfoSubf(p.sub, "Workshop.CollectRoutine id: %d is closing", id)
			break LOOP
		}
		select {
		case cmd := <-ctrlChannel:
			if cmd == CMD_EXIT {
				logger.InfoSubf(p.sub, "Workshop.CollectRoutine id: %d receive exit cmd", id)
				exitFlag = true
			} else if cmd == CMD_TICK {
				for _, collector := range collectorMap {
					collector.Tick()
				}
			} else {
				logger.ErrorSubf(p.sub, "Workshop.CollectRoutine id: %d receive a un-expected cmd: %v",
					id, cmd)
			}
		case processed_item := <-channel:
			category := processed_item.Category
			if collector, ok := collectorMap[category]; ok {
				if err := collector.Collect(processed_item); err != nil {
					logger.ErrorSubf(p.sub, "Workshop.CollectRoutine id: %d %s collect fail, err: %v",
						id, category, err)
				}
			} else {
				logger.ErrorSubf(p.sub, "Workshop.CollectRoutine id: %d data %s has no proper collector: %v",
					id, category, collectorMap)
			}
		case <-time.After(time.Second * 1):
			// logger.Debug(id, "th CollectRoutine nothing to do")
		} // select
	} // for

	for _, collector := range collectorMap {
		collector.Destory()
	}
	p.CLReportChan <- RET_EXIT_SUCCESS
}

func (p *Workshop) worker() {
	// 工作goroutine
	logger.InfoSubf(p.sub, "WorkShop.worker begin")
	exitedProcesser := 0
	exitedCollector := 0
	for {
		select {
		// 通过workCtrlChan从控制goroutine获取指令，没有指令的时候就干活
		case cmd := <-p.workCtrlChan:
			logger.InfoSubf(p.sub, "WorkShop.worker get a cmd: %v", cmd)
			if cmd == CMD_EXIT {
				logger.InfoSubf(p.sub, "Workshop.worker quit, no msg will push to msgChan, stock will go on")
				// 告诉processor关张
				// 这一步可能阻塞，所有需要阻塞的工作都交给worker干

				// before
				// 顺序并阻塞的发送退出信号，太慢了
				// after
				// 不断try广播，这样可以尽可能使线程早日得到退出信号，加速平滑退出的速度
				func() {
					p.trySendCtrlInfo(p.PRCtrlChans, cmd)
					for {
						select {
						case <-p.PRReportChan:
							exitedProcesser++
							logger.InfoSubf(p.sub, "WorkShop.worker receive a exit from processer, total: %d",
								exitedProcesser)
							if exitedProcesser >= p.processorNum {
								return
							}
						case <-time.After(time.Second * 5):
							logger.InfoSubf(p.sub, "WorkShop.worker after some time from close processer, total: %d",
								exitedProcesser)
							p.trySendCtrlInfo(p.PRCtrlChans, cmd)
						}
					}
				}()

				func() {
					p.trySendCtrlInfo(p.CLCtrlChans, cmd)
					for {
						select {
						case <-p.CLReportChan:
							exitedCollector++
							logger.InfoSubf(p.sub, "WorkShop.worker receive a exit from collector, total: %d",
								exitedCollector)
							if exitedCollector >= p.collectorNum {
								p.workReportChan <- RET_EXIT_SUCCESS
								return
							}
						case <-time.After(time.Second * 5):
							logger.InfoSubf(p.sub, "WorkShop.worker after some time from close collector, total: %d",
								exitedCollector)
							p.trySendCtrlInfo(p.CLCtrlChans, cmd)
						}
					}
				}()
				return
			} else {
				logger.WarnSubf(p.sub, "Workshop.worker bad cmd type: %v", cmd)
			}
		default:
			// how many msgs it returns each time depend on max_messsage_fetch_size in etc/qbus-client.conf
			messages, err := p.provider.GetNextMsg()
			if err != nil {
				logger.ErrorSubf(p.sub, "WorkShop.worker GetNextMsg error: %v", err)
				// 等一会儿再取消息
				time.Sleep(1 * time.Second)
				continue
			}
			if messages == nil || len(messages) <= 0 {
				// 如果没有消息需要处理，说明比较闲，就sleep一下
				logger.InfoSubf(p.sub, "WorkShop.worker receive nothing from srcProvider")
				time.Sleep(5 * time.Second)
				continue
			}
			logger.InfoSubf(p.sub, "WorkShop.worker receive %d msgs from srcProvider", len(messages))
			for _, v := range messages {
				if len(v) > 0 {
					p.msgChan <- string(v)
				}
			}
			logger.InfoSubf(p.sub, "WorkShop.worker send %d msgs successful", len(messages))
			p.processedLine += len(messages)
			p.provider.Ack()
		}
	}
}

func (p *Workshop) Run() {
	// 分两个goroutine，一个控制，不许阻塞，一个工作可以阻塞
	go p.worker()

	isWorking := true
	// 控制线程是主线程，具有退出的权利
LOOP:
	for {
		select {
		case cmd := <-p.ctrlChan:
			if cmd == CMD_EXIT {
				if !isWorking {
					logger.InfoSubf(p.sub, "Workshop.Run control goroutine is going home, ignore")
					continue
				}
				logger.InfoSubf(p.sub, "Workshop.Run control goroutine tell work goroutine to go home")
				isWorking = false
				// 由于workCtrlChan有一个空间，所以正常不会堵
				// worker接到这个命令后会通知processor关闭，这个动作是可能阻塞的
				// 这里要确保所有processor都关闭后，再关闭collector
				p.workCtrlChan <- CMD_EXIT

				// 从此以后是否不再给processor和collector打tick，抢夺worker向其打exit的机会
				// ticker还要，但是不给下游发
				// p.ticker_.Stop()
			} else {
				logger.WarnSubf(p.sub, "Workshop.Run bad cmd type: %v", cmd)
			}
		case <-p.workReportChan:
			logger.InfoSubf(p.sub, "Workshop.Run worker report exit successful, so exit")
			break LOOP
		case <-p.ticker_.C:
			logger.InfoSubf(p.sub, "Workshop.Run tick begin")
			// 不许阻塞
			if isWorking {
				p.trySendCtrlInfo(p.PRCtrlChans, CMD_TICK)
				p.trySendCtrlInfo(p.CLCtrlChans, CMD_TICK)
			}
			p.Tick()
		}
	}

	// all destory
	p.provider.Destory()
	p.reportChan <- RET_EXIT_SUCCESS
}

var lastProcessedLine = make(map[string]int, 0)

func (p *Workshop) Tick() error {
	logger.InfoSubf(p.sub, "Workshop.Tick receive %d(current tick), %d(totally) msgs",
		p.processedLine-lastProcessedLine[p.sub], p.processedLine)
	lastProcessedLine[p.sub] = p.processedLine

	p.ChannelStat()
	// p.GC()
	return nil
}

func (p *Workshop) ChannelStat() {
	logger.InfoSubf(p.sub, "Workshop.ChannelStat len(ctrlChan): %d", len(p.ctrlChan))
	logger.InfoSubf(p.sub, "Workshop.ChannelStat len(workCtrlChan): %d", len(p.workCtrlChan))
	logger.InfoSubf(p.sub, "Workshop.ChannelStat len(msgChan): %d", len(p.msgChan))
	for i, itemChan := range p.itemChans {
		logger.InfoSubf(p.sub, "Workshop.ChannelStat index: %d, len(itemChan): %d", i, len(itemChan))
	}
}

// func (p *Workshop) GC() {
// 	runtime.ReadMemStats(&(p.ms))
// 	alloc := p.ms.Alloc / 1024 / 1024
// 	logger.Info("memAlloc:", alloc, "M heapAlloc:", p.ms.HeapAlloc/1024/1024, "M, stackAlloc:", p.ms.StackInuse/1024/1024, "M")
// 	if alloc >= p.memLimit {
// 		debug.FreeOSMemory()
// 		runtime.ReadMemStats(&(p.ms))
// 		alloc = p.ms.Alloc / 1024 / 1024
// 		logger.Info("after GC memAlloc:", alloc, "M heapAlloc:", p.ms.HeapAlloc/1024/1024, "M, stackAlloc:", p.ms.StackInuse/1024/1024, "M")
// 	}
// }

func (p *Workshop) initSrcProvider() error {
	ctxSrcProvider := p.ctx.Get("src_provider")
	if ctxSrcProvider == nil {
		return errors.New("no src_provider section")
	}
	srcType := ctxSrcProvider.Get("src_type").MustString()
	p.provider = NewSrcProvider(srcType)
	if p.provider == nil {
		return errors.New("provider create failed")
	}
	ctxProviderDetail := ctxSrcProvider.Get(srcType)
	if ctxProviderDetail == nil {
		return errors.New("no src_provider detail section " + srcType)
	}
	if err := p.provider.Init(ctxProviderDetail); err != nil {
		return err
	}
	logger.InfoSubf(p.sub, "Workshop.initSrcProvider success, srcType: %s", srcType)
	return nil
}

// 会阻塞但保证能发送成功
func (p *Workshop) sendCtrlInfo(channels [](chan int), cmd int) {
	logger.InfoSubf(p.sub, "Workshop.sendCtrlInfo begin")
	for _, channel := range channels {
		channel <- cmd
	}
	logger.InfoSubf(p.sub, "Workshop.sendCtrlInfo end")
}

// 不会阻塞但不保证能发送成功
func (p *Workshop) trySendCtrlInfo(channels [](chan int), cmd int) {
	logger.InfoSubf(p.sub, "Workshop.trySendCtrlInfo begin")
	for i, channel := range channels {
		select {
		case channel <- cmd:
		default:
			logger.InfoSubf(p.sub, "Workshop.trySendCtrlInfo %d th channel full", i)
		}
	}
	logger.InfoSubf(p.sub, "Workshop.trySendCtrlInfo end")
}
