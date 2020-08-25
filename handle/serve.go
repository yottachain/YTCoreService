package handle

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var AYNC_ROUTINE_NUM *int32 = new(int32)
var SYNC_ROUTINE_NUM *int32 = new(int32)
var READ_ROUTINE_NUM *int32 = new(int32)
var WRITE_ROUTINE_NUM *int32 = new(int32)
var STAT_ROUTINE_NUM *int32 = new(int32)

func Start() {
	OBJ_LIST_CACHE = cache.New(time.Duration(env.LsCacheExpireTime)*time.Second, time.Duration(5)*time.Second)
	atomic.StoreInt32(AYNC_ROUTINE_NUM, 0)
	atomic.StoreInt32(SYNC_ROUTINE_NUM, 0)
	atomic.StoreInt32(READ_ROUTINE_NUM, 0)
	atomic.StoreInt32(WRITE_ROUTINE_NUM, 0)
	atomic.StoreInt32(STAT_ROUTINE_NUM, 0)
	if env.STAT_SERVICE {
		InitSpotCheckService()
		InitRebuildService()
		//go StartSyncNodes()
		go StartDoCacheFee()
		go StartSumUsedSpace()
		go StartIterateShards()
		go StartIterateUser()
		go StartDNBlackListCheck()
	}
}

type MessageEvent interface {
	Handle() proto.Message
	SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32)
}

func FindHandler(msg proto.Message) (MessageEvent, *pkt.ErrorMessage) {
	return findHandler(msg, 0)
}

func findHandler(msg proto.Message, msgType uint16) (MessageEvent, *pkt.ErrorMessage) {
	var mtype uint16
	if msgType == 0 {
		name := reflect.Indirect(reflect.ValueOf(msg)).Type().Name()
		id, err := pkt.GetMessageID(name)
		if err != nil {
			return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
		}
		mtype = uint16(id)
	} else {
		mtype = msgType
	}
	handfunc, ok := ID_HANDLER_MAP[mtype]
	if !ok {
		name := reflect.Indirect(reflect.ValueOf(msg)).Type().Name()
		emsg := fmt.Sprintf("Invalid instruction:%d<-->%s\n", mtype, name)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, emsg)
	}
	return handfunc(), nil
}

func OnMessage(msgType uint16, data []byte, pubkey string) []byte {
	msgfunc, ok := pkt.ID_CLASS_MAP[msgType]
	if !ok {
		logrus.Errorf("[OnMessage]Invalid msgid:%d\n", msgType)
		return pkt.ErrorMsg(pkt.INVALID_ARGS, fmt.Sprintf("Invalid msgid:%d", msgType))
	}
	msg := msgfunc()
	name := reflect.Indirect(reflect.ValueOf(msg)).Type().Name()
	defer env.TracePanic()
	err := proto.Unmarshal(data, msg)
	if err != nil {
		logrus.Errorf("[OnMessage]Deserialize (Msgid:%d) ERR:%s\n", msgType, err.Error())
		return pkt.ErrorMsg(pkt.INVALID_ARGS, fmt.Sprintf("Deserialize (Msgid:%d) ERR:%s", msgType, err.Error()))
	}
	handler, err1 := findHandler(msg, msgType)
	if err1 != nil {
		return pkt.MarshalError(err1)
	}
	err2, rnum, urnum := handler.SetMessage(pubkey, msg)
	if err2 != nil {
		return pkt.MarshalMsgBytes(err2)
	}
	var curRouteNum int32 = 0
	if rnum != nil {
		err = CheckRoutine(rnum)
		if err != nil {
			logrus.Errorf("[OnMessage]%s,ERR:%s\n", name, err)
			return pkt.MarshalMsgBytes(pkt.BUSY_ERROR)
		}
		curRouteNum = atomic.AddInt32(rnum, 1)
		defer atomic.AddInt32(rnum, -1)
	}
	if urnum != nil {
		if atomic.LoadInt32(urnum) > env.PER_USER_MAX_READ_ROUTINE {
			logrus.Warnf("[OnMessage]%s,The current user's concurrent read has reached the upper limit\n", name)
			return pkt.MarshalMsgBytes(pkt.BUSY_ERROR)
		}
		atomic.AddInt32(urnum, 1)
		defer atomic.AddInt32(urnum, -1)
	}
	startTime := time.Now()
	res := handler.Handle()
	stime := time.Now().Sub(startTime).Milliseconds()
	if stime > int64(env.SLOW_OP_TIMES) {
		logrus.Infof("[OnMessage]%s,routine num %d,take times %d ms\n", name, curRouteNum, stime)
	}
	return pkt.MarshalMsgBytes(res)
}

func CheckRoutine(rnum *int32) error {
	if WRITE_ROUTINE_NUM == rnum {
		if atomic.LoadInt32(WRITE_ROUTINE_NUM) > env.MAX_WRITE_ROUTINE {
			return errors.New("WRITE_ROUTINE:Too many routines")
		}
	} else if SYNC_ROUTINE_NUM == rnum {
		if atomic.LoadInt32(SYNC_ROUTINE_NUM) > env.MAX_SYNC_ROUTINE {
			return errors.New("SYNC_ROUTINE:Too many routines")
		}
	} else if READ_ROUTINE_NUM == rnum {
		if atomic.LoadInt32(READ_ROUTINE_NUM) > env.MAX_READ_ROUTINE {
			return errors.New("READ_ROUTINE:Too many routines")
		}
	} else if STAT_ROUTINE_NUM == rnum {
		if atomic.LoadInt32(STAT_ROUTINE_NUM) > env.MAX_STAT_ROUTINE {
			return errors.New("STAT_ROUTINE:Too many routines")
		}
	} else {
		if atomic.LoadInt32(AYNC_ROUTINE_NUM) > env.MAX_AYNC_ROUTINE {
			return errors.New("AYNC_ROUTINE:Too many routines")
		}
	}
	return nil
}
