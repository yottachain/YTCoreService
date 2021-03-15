package handle

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
)

var RoutineConter *int32 = new(int32)

func AyncRequest(reqmsg proto.Message, exclude int, retrytime int) error {
	if atomic.LoadInt32(RoutineConter) > env.MAX_AYNC_ROUTINE {
		return errors.New("Too many routines.")
	}
	list := net.GetSuperNodes()
	for index, node := range list {
		if index != exclude {
			sy := &SNSynchronizer{
				req:        reqmsg,
				sn:         node,
				retryTimes: retrytime,
				wg:         nil,
			}
			go sy.run()
		}
	}
	return nil
}

func SyncRequestSN(reqmsg proto.Message, include []int, retrytime int) ([]*SNSynchronizer, error) {
	if atomic.LoadInt32(RoutineConter) > env.MAX_AYNC_ROUTINE {
		return nil, errors.New("AyncRequest:Too many routines.")
	}
	list := []*YTDNMgmt.SuperNode{}
	for _, id := range include {
		sn := net.GetSuperNode(id)
		if sn != nil {
			list = append(list, sn)
		} else {
			return nil, fmt.Errorf("Invalid SNID:%d", id)
		}
	}
	num := len(list)
	syncrun := make([]*SNSynchronizer, num)
	if num <= 0 {
		return syncrun, nil
	}
	wgroup := sync.WaitGroup{}
	wgroup.Add(num)
	for index, node := range list {
		sy := &SNSynchronizer{
			req:        reqmsg,
			sn:         node,
			retryTimes: retrytime,
			wg:         &wgroup,
		}
		syncrun[index] = sy
		go sy.run()

	}
	wgroup.Wait()
	return syncrun, nil
}

func SyncRequest(reqmsg proto.Message, exclude int, retrytime int) ([]*SNSynchronizer, error) {
	if atomic.LoadInt32(RoutineConter) > env.MAX_AYNC_ROUTINE {
		return nil, errors.New("AyncRequest:Too many routines.")
	}
	list := net.GetSuperNodes()
	num := len(list)
	syncrun := make([]*SNSynchronizer, num)
	if exclude >= 0 && exclude < num {
		num--
	}
	if num <= 0 {
		return syncrun, nil
	}
	wgroup := sync.WaitGroup{}
	wgroup.Add(num)
	for index, node := range list {
		if index != exclude {
			sy := &SNSynchronizer{
				req:        reqmsg,
				sn:         node,
				retryTimes: retrytime,
				wg:         &wgroup,
			}
			syncrun[index] = sy
			go sy.run()
		}
	}
	wgroup.Wait()
	return syncrun, nil
}

type SNSynchronizer struct {
	req        proto.Message
	resp       proto.Message
	err        *pkt.ErrorMessage
	sn         *YTDNMgmt.SuperNode
	retryTimes int
	wg         *sync.WaitGroup
}

func (self *SNSynchronizer) GetSN() *YTDNMgmt.SuperNode {
	return self.sn
}

func (self *SNSynchronizer) Response() proto.Message {
	return self.resp
}

func (self *SNSynchronizer) Error() *pkt.ErrorMessage {
	return self.err
}

func (self *SNSynchronizer) dofinish() {
	env.TracePanic("[SyncMsg]")
	if self.wg != nil {
		self.wg.Done()
	}
	atomic.AddInt32(RoutineConter, -1)
}

func (self *SNSynchronizer) run() {
	atomic.AddInt32(RoutineConter, 1)
	defer self.dofinish()
	for {
		if self.sn.ID == int32(env.SuperNodeID) {
			handler, err := FindHandler(self.req)
			if err != nil {
				self.err = err
				return
			}
			err1, _, _ := handler.SetMessage(self.sn.PubKey, self.req)
			if err1 != nil {
				self.err = err1
				return
			}
			res := handler.Handle()
			if errmsg, ok := res.(*pkt.ErrorMessage); ok {
				self.err = errmsg
			} else {
				self.resp = res
				return
			}
		} else {
			res, err := net.RequestSN(self.req, self.sn, "", 0, true)
			if err != nil {
				self.err = err
			} else {
				self.resp = res
				return
			}
		}
		name := reflect.Indirect(reflect.ValueOf(self.req)).Type().Name()
		logrus.Errorf("[SyncMsg]Sync %s to %d ErrCode:%d,ERR:%s\n", name, self.sn.ID, self.err.Code, self.err.Msg)
		if self.retryTimes == 0 {
			return
		} else {
			time.Sleep(time.Duration(5) * time.Second)
		}
		self.retryTimes--
	}
}
