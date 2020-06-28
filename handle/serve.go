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

var ROUTINE_SIZE *int32 = new(int32)

const MAX_ROUTINE_SIZE = 3000

func Start() {
	atomic.StoreInt32(ROUTINE_SIZE, 0)
	go SumUsedSpace()
	go DoNodeStatSyncLoop()
	go DoCacheActionLoop()
	InitSpotCheckService()
}

type MessageEvent interface {
	Handle() proto.Message
	SetPubkey(pubkey string)
	SetMessage(msg proto.Message) *pkt.ErrorMessage
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
	handler := handfunc()
	err1 := handler.SetMessage(msg)
	if err1 != nil {
		return nil, err1
	}
	return handler, nil
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
	startTime := time.Now()
	handler, err1 := findHandler(msg, msgType)
	if err1 != nil {
		return pkt.MarshalError(err1)
	}
	handler.SetPubkey(pubkey)
	res := handler.Handle()
	stime := time.Now().Sub(startTime).Milliseconds()
	if stime > 50 {
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
