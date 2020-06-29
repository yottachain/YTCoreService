package handle

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var AYNC_ROUTINE_NUM *int32 = new(int32)
var READ_ROUTINE_NUM *int32 = new(int32)
var WRITE_ROUTINE_NUM *int32 = new(int32)
var STAT_ROUTINE_NUM *int32 = new(int32)

func Start() {
	atomic.StoreInt32(AYNC_ROUTINE_NUM, 0)
	atomic.StoreInt32(READ_ROUTINE_NUM, 0)
	atomic.StoreInt32(WRITE_ROUTINE_NUM, 0)
	atomic.StoreInt32(STAT_ROUTINE_NUM, 0)
	go SumUsedSpace()
	go DoNodeStatSyncLoop()
	go DoCacheActionLoop()
	InitSpotCheckService()
	InitRebuildService()
}

type MessageEvent interface {
	Handle() proto.Message
	SetMessage(pubkey string, msg proto.Message) *pkt.ErrorMessage
	CheckRoutine() *int32
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
		env.Log.Errorf(emsg)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, emsg)
	}
	return handfunc(), nil
}

func OnError(msg proto.Message, name string) {
	if r := recover(); r != nil {
		env.Log.Tracef("OnMessage %s ERR:%s\n", name, r)
	}
}

func OnMessage(msgType uint16, data []byte, pubkey string) []byte {
	msgfunc, ok := pkt.ID_CLASS_MAP[msgType]
	if !ok {
		env.Log.Errorf("Invalid msgid:%d\n", msgType)
		return pkt.ErrorMsg(pkt.INVALID_ARGS, fmt.Sprintf("Invalid msgid:%d", msgType))
	}
	msg := msgfunc()
	name := reflect.Indirect(reflect.ValueOf(msg)).Type().Name()
	defer OnError(msg, name)
	err := proto.Unmarshal(data, msg)
	if err != nil {
		env.Log.Errorf("Deserialize (Msgid:%d) ERR:%s\n", msgType, err.Error())
		return pkt.ErrorMsg(pkt.INVALID_ARGS, fmt.Sprintf("Deserialize (Msgid:%d) ERR:%s", msgType, err.Error()))
	}
	handler, err1 := findHandler(msg, msgType)
	if err1 != nil {
		return pkt.MarshalError(err1)
	}
	err2 := handler.SetMessage(pubkey, msg)
	if err2 != nil {
		return pkt.MarshalMsgBytes(err2)
	}
	rnum := handler.CheckRoutine()
	if rnum == nil {
		env.Log.Errorf("OnMessage %s ERR:Too many routines\n", name)
		return pkt.MarshalMsgBytes(pkt.BUSY_ERROR)
	}
	defer atomic.AddInt32(rnum, -1)
	startTime := time.Now()
	res := handler.Handle()
	stime := time.Now().Sub(startTime).Milliseconds()
	if stime > int64(env.SLOW_OP_TIMES) {
		env.Log.Infof("OnMessage %s take times %d ms\n", name, stime)
	}
	return pkt.MarshalMsgBytes(res)
}

func IsExistInArray(id int32, array []int32) bool {
	for _, arr := range array {
		if id == arr {
			return true
		}
	}
	return false
}
