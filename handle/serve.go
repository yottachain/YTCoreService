package handle

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var AYNC_ROUTINE_NUM *int32 = new(int32)
var SYNC_ROUTINE_NUM *int32 = new(int32)
var READ_ROUTINE_NUM *int32 = new(int32)
var WRITE_ROUTINE_NUM *int32 = new(int32)
var STAT_ROUTINE_NUM *int32 = new(int32)
var HTTP_ROUTINE_NUM *int32 = new(int32)
var SUMFEE_ROUTINE_NUM *int32 = new(int32)
var DELBLK_ROUTINE_NUM *int32 = new(int32)
var AUTH_ROUTINE_NUM *int32 = new(int32)

func Start() {
	InitCache()
	atomic.StoreInt32(AYNC_ROUTINE_NUM, 0)
	atomic.StoreInt32(SYNC_ROUTINE_NUM, 0)
	atomic.StoreInt32(READ_ROUTINE_NUM, 0)
	atomic.StoreInt32(WRITE_ROUTINE_NUM, 0)
	atomic.StoreInt32(STAT_ROUTINE_NUM, 0)
	atomic.StoreInt32(HTTP_ROUTINE_NUM, 0)
	atomic.StoreInt32(SUMFEE_ROUTINE_NUM, 0)
	atomic.StoreInt32(DELBLK_ROUTINE_NUM, 0)
	atomic.StoreInt32(AUTH_ROUTINE_NUM, 0)
	if env.STAT_SERVICE {
		InitSpotCheckService()
		InitRebuildService()
		//go StartSyncNodes()
		go StartDoCacheFee()
		go StartSumUsedSpace()
		go StartIterateShards()
		go StartIterateUser()
		go StartDNBlackListCheck()
		go StartDoDelete()
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
	defer env.TracePanic("[OnMessage]")
	err := proto.Unmarshal(data, msg)
	if err != nil {
		logrus.Errorf("[OnMessage]Deserialize (Msgid:%d) ERR:%s\n", msgType, err.Error())
		return pkt.ErrorMsg(pkt.INVALID_ARGS, fmt.Sprintf("Deserialize (Msgid:%d) ERR:%s", msgType, err.Error()))
	}
	handler, err1 := findHandler(msg, msgType)
	if err1 != nil {
		logrus.Errorf("[OnMessage]FindHandler %s %s\n", name, pkt.ToError(err1))
		return pkt.MarshalError(err1)
	}
	err2, rnum, urnum := handler.SetMessage(pubkey, msg)
	if err2 != nil {
		logrus.Errorf("[OnMessage]SetMessage %s %s,data len:%d\n", name, pkt.ToError(err2), len(data))
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
	} else if HTTP_ROUTINE_NUM == rnum {
		if atomic.LoadInt32(HTTP_ROUTINE_NUM) > env.MAX_HTTP_ROUTINE {
			return errors.New("HTTP_ROUTINE:Too many routines")
		}
	} else if SUMFEE_ROUTINE_NUM == rnum {
		if atomic.LoadInt32(SUMFEE_ROUTINE_NUM) > env.MAX_SUMFEE_ROUTINE {
			return errors.New("SUMFEE_ROUTINE:Too many routines")
		}
	} else if DELBLK_ROUTINE_NUM == rnum {
		if atomic.LoadInt32(DELBLK_ROUTINE_NUM) > env.MAX_DELBLK_ROUTINE {
			return errors.New("DELBLK_ROUTINE:Too many routines")
		}
	} else if AUTH_ROUTINE_NUM == rnum {
		if atomic.LoadInt32(AUTH_ROUTINE_NUM) > env.MAX_AUTH_ROUTINE {
			return errors.New("AUTH_ROUTINE:Too many routines")
		}
	} else {
		if atomic.LoadInt32(AYNC_ROUTINE_NUM) > env.MAX_AYNC_ROUTINE {
			return errors.New("AYNC_ROUTINE:Too many routines")
		}
	}
	return nil
}
