package net

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
)

type Node struct {
	Id     int32
	Nodeid string
	Pubkey string
	Addrs  []string
}

func RequestDN(msg proto.Message, dn *Node, log_prefix string) (proto.Message, *pkt.ErrorMessage) {
	data, name, msgtype, merr := pkt.MarshalMsg(msg)
	if merr != nil {
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, merr.Error())
	}
	var log_pre string
	if log_prefix == "" {
		log_pre = fmt.Sprintf("[%s][%d]", name, dn.Id)
	} else {
		log_pre = fmt.Sprintf("[%s][%d]%s", name, dn.Id, log_prefix)
	}
	client, err := NewClient(dn.Nodeid, false)
	if err != nil {
		return nil, err
	}
	return client.Request(int32(msgtype), data, dn.Addrs, log_pre)
}

func RequestSN(msg proto.Message, sn *YTDNMgmt.SuperNode, log_prefix string, retry int, nowait bool) (proto.Message, *pkt.ErrorMessage) {
	data, name, msgtype, merr := pkt.MarshalMsg(msg)
	if merr != nil {
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, merr.Error())
	}
	var log_pre string
	if log_prefix == "" {
		log_pre = fmt.Sprintf("[%s][%d]", name, sn.ID)
	} else {
		log_pre = fmt.Sprintf("[%s][%d]%s", name, sn.ID, log_prefix)
	}
	retryTimes := 0
	for {
		if retryTimes > 1 {
			logrus.Infof("[P2P]%sRetry...\n", log_pre)
		}
		client, err := NewClient(sn.NodeID, nowait)
		if err != nil {
			return nil, err
		}
		resmsg, err := client.Request(int32(msgtype), data, sn.Addrs, log_pre)
		if err != nil {
			if nowait || retryTimes >= retry {
				return nil, err
			}
			if !(err.Code == pkt.COMM_ERROR || err.Code == pkt.SERVER_ERROR) {
				return nil, err
			}
			if retryTimes != 0 {
				time.Sleep(time.Duration(env.SN_RETRY_WAIT) * time.Second)
			}
			retryTimes++
		} else {
			return resmsg, nil
		}
	}
}
