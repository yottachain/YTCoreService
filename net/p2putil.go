package net

import (
	"fmt"
	"strings"
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
	Weight float64
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
	client, err := NewClient(dn.Nodeid)
	if err != nil {
		return nil, err
	}
	return client.Request(int32(msgtype), data, dn.Addrs, log_pre, false)
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
		snclient, err := NewSNClient(sn)
		if err != nil {
			return nil, err
		}
		var resmsg proto.Message
		var errmsg *pkt.ErrorMessage
		if snclient.HttpSupported {
			resmsg, errmsg = snclient.Request(int32(msgtype), data, log_pre, nowait)
		} else {
			client, err := NewClient(sn.NodeID)
			if err != nil {
				return nil, err
			}
			resmsg, errmsg = client.Request(int32(msgtype), data, snclient.TcpAddr, log_pre, nowait)
		}
		if errmsg != nil {
			if !(errmsg.Code == pkt.COMM_ERROR || errmsg.Code == pkt.SERVER_ERROR || errmsg.Code == pkt.CONN_ERROR) {
				return nil, errmsg
			}
			if nowait || retryTimes >= retry {
				return nil, errmsg
			} else {
				logrus.Errorf("[P2P]%sServiceError %d:%s,Retry...\n", log_pre, errmsg.Code, strings.TrimSpace(errmsg.Msg))
			}
			if !(retryTimes == 0 && (errmsg.Code == pkt.COMM_ERROR || errmsg.Code == pkt.CONN_ERROR)) {
				time.Sleep(time.Duration(env.SN_RETRY_WAIT) * time.Second)
			}
			retryTimes++
		} else {
			return resmsg, nil
		}
	}
}
