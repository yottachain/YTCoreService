package net

import (
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"google.golang.org/protobuf/proto"
)

var snclient *SNClient

func startSnClient(pid string, addrs []string) *pkt.ErrorMessage {
	if snclient != nil {
		return nil
	}
	startHttpClient()
	h := &SNClient{}
	id, err := peer.Decode(pid)
	if err != nil {
		logmsg := fmt.Sprintf("PeerID %s INVALID:%s", pid, err.Error())
		logrus.Errorf("[SnClient]%s\n", logmsg)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, logmsg)
	}
	h.PeerId = id
	for _, addr := range addrs {
		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			logrus.Warnf("[SnClient]INVALID_ADDR:%s\n", addr)
			continue
		}
		if _, err := maddr.ValueForProtocol(ma.P_HTTP); err == nil {
			h.HttpSupported = true
			h.HttpMultiAddr = append(h.HttpMultiAddr, maddr)
		} else {
			h.TcpAddr = append(h.TcpAddr, maddr)
		}
	}
	snclient = h
	return nil
}

type SNClient struct {
	PeerId        peer.ID
	HttpSupported bool
	HttpMultiAddr []ma.Multiaddr
	TcpAddr       []ma.Multiaddr
}

func (me *SNClient) Request(msgid int32, data []byte, log_pre string) (proto.Message, *pkt.ErrorMessage) {
	for index, maddr := range me.HttpMultiAddr {
		res, serr := SendHTTPMsg(me.PeerId, maddr, msgid, data)
		if serr != nil {
			logmsg := fmt.Sprintf("Request %s,COMM_ERROR:%s\n", maddr, serr.Error())
			logrus.Errorf("[SnClient]%s%s\n", log_pre, logmsg)
			if index == len(me.HttpMultiAddr)-1 {
				return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
			}
			continue
		} else {
			msg := pkt.UnmarshalMsg(res)
			if errmsg, ok := msg.(*pkt.ErrorMessage); ok {
				return nil, errmsg
			} else {
				return msg, nil
			}
		}
	}
	return nil, nil
}

func RequestSN(msg proto.Message) (proto.Message, *pkt.ErrorMessage) {
	data, name, msgtype, merr := pkt.MarshalMsg(msg)
	if merr != nil {
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, merr.Error())
	}
	log_pre := fmt.Sprintf("[%s]", name)
	retryTimes := 0
	for {
		var resmsg proto.Message
		var errmsg *pkt.ErrorMessage
		if snclient.HttpSupported {
			resmsg, errmsg = snclient.Request(int32(msgtype), data, log_pre)
		} else {
			resmsg, errmsg = DoRequest(msg, snclient.PeerId, snclient.TcpAddr)
		}
		if errmsg != nil {
			if !(errmsg.Code == pkt.COMM_ERROR || errmsg.Code == pkt.SERVER_ERROR || errmsg.Code == pkt.CONN_ERROR) {
				return nil, errmsg
			}
			if retryTimes >= env.SN_RETRY_TIMES {
				return nil, errmsg
			} else {
				logrus.Errorf("[SnClient]%sServiceError %d:%s,Retry...\n", log_pre, errmsg.Code, strings.TrimSpace(errmsg.Msg))
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
