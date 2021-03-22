package handle

import (
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yottachain/YTCoreService/env"
)

func init() {
	go func() {
		for {
			if ClearNodeLog() {
				time.Sleep(time.Duration(60) * time.Second)
			}
		}
	}()
}

var LOGS = struct {
	sync.RWMutex
	NodeMAP map[int32]*NodeLog
}{NodeMAP: make(map[int32]*NodeLog)}

func ClearNodeLog() bool {
	var del_id int32 = -1
	var del_c *NodeLog
	LOGS.RLock()
	count := len(LOGS.NodeMAP)
	if count < 1000 {
		LOGS.RUnlock()
		return true
	}
	for k, v := range LOGS.NodeMAP {
		if !v.IsActive() {
			del_id = k
			del_c = v
			break
		}
	}
	if del_id == -1 {
		LOGS.RUnlock()
		return true
	} else {
		LOGS.RUnlock()
		LOGS.Lock()
		delete(LOGS.NodeMAP, del_id)
		del_c.Close()
		LOGS.Unlock()
		return false
	}
}

func GetNodeLog(id int32) (*NodeLog, error) {
	var log *NodeLog
	LOGS.RLock()
	log = LOGS.NodeMAP[id]
	LOGS.RUnlock()
	if log != nil {
		return log, nil
	}
	LOGS.Lock()
	log = LOGS.NodeMAP[id]
	if log == nil {
		log = NewNodeLog(id, env.DelLogPath)
		LOGS.NodeMAP[id] = log
	}
	LOGS.Unlock()
	return log, log.CalCurDate()
}

type NodeLog struct {
	sync.RWMutex
	EndTime    *int64
	NodeId     int32
	path       string
	writer     io.WriteCloser
	activeTime *int64
}

func NewNodeLog(id int32, path string) *NodeLog {
	log := &NodeLog{}
	log.NodeId = id
	log.path = path
	log.EndTime = new(int64)
	begin := time.Now().Unix()
	log.activeTime = &begin
	return log
}

func (me *NodeLog) CalCurDate() error {
	me.Lock()
	defer me.Unlock()
	t := time.Now()
	if t.Unix() < atomic.LoadInt64(me.EndTime) {
		return nil
	}
	if me.writer != nil {
		me.writer.Close()
		me.writer = nil
	}
	t = t.Add(-time.Hour * 24 * (time.Duration(t.Weekday())))
	endts := t.Unix() / (60 * 60 * 24)
	endts = (endts + 7) * (60 * 60 * 24)
	atomic.StoreInt64(me.EndTime, endts)
	CurDate := t.Format("20060102")
	dirname := me.path + CurDate
	err := os.MkdirAll(dirname, os.ModePerm)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(dirname+"/"+strconv.Itoa(int(me.NodeId)), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	me.writer = f
	atomic.StoreInt64(me.activeTime, time.Now().Unix())
	return nil
}

func (me *NodeLog) WriteLog(dat string) error {
	t := time.Now()
	if t.Unix() >= atomic.LoadInt64(me.EndTime) {
		err := me.CalCurDate()
		if err != nil {
			return err
		}
	}
	_, err := me.writer.Write([]byte(dat + "\n"))
	if err != nil {
		return err
	}
	atomic.StoreInt64(me.activeTime, time.Now().Unix())
	return nil
}

func (me *NodeLog) Close() {
	if me.writer != nil {
		me.writer.Close()
		me.writer = nil
	}
}

const LOG_OPEN_EXPIRED = 60 * 10

func (me *NodeLog) IsActive() bool {
	return time.Now().Unix()-atomic.LoadInt64(me.activeTime) <= LOG_OPEN_EXPIRED
}
