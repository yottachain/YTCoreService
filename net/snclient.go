package net

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	"github.com/yottachain/YTHost/client"
)

var SN_MAP sync.Map

type SNClient struct {
	PeerId        peer.ID
	HttpSupported bool
	HttpMultiAddr []ma.Multiaddr
	TcpAddr       []ma.Multiaddr
}

func NewSNClient(sn *YTDNMgmt.SuperNode) (*SNClient, *pkt.ErrorMessage) {
	http, ok := SN_MAP.Load(sn.NodeID)
	if ok {
		return http.(*SNClient), nil
	}
	h := &SNClient{}
	id, err := peer.Decode(sn.NodeID)
	if err != nil {
		logmsg := fmt.Sprintf("PeerID %s INVALID:%s", sn.NodeID, err.Error())
		logrus.Errorf("[P2P]%s\n", logmsg)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, logmsg)
	}
	h.PeerId = id
	for _, addr := range sn.Addrs {
		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			logrus.Warnf("[P2P]INVALID_ADDR:%s\n", addr)
			continue
		}
		if _, err := maddr.ValueForProtocol(ma.P_HTTP); err == nil {
			h.HttpSupported = true
			h.HttpMultiAddr = append(h.HttpMultiAddr, maddr)
		} else {
			h.TcpAddr = append(h.TcpAddr, maddr)
		}
	}
	SN_MAP.Store(sn.NodeID, h)
	return h, nil
}

func (me *SNClient) Request(msgid int32, data []byte, log_pre string, nowait bool) (proto.Message, *pkt.ErrorMessage) {
	timeout := time.Millisecond * time.Duration(client.ReadTimeout)
	if nowait {
		timeout = time.Millisecond * time.Duration(client.WriteTimeout)
	}
	for index, maddr := range me.HttpMultiAddr {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		res, serr := p2phst.SendMsgAuto(ctx, me.PeerId, msgid, maddr, data)
		cancel()
		if serr != nil {
			logmsg := fmt.Sprintf("Request %s,COMM_ERROR:%s\n", maddr, serr.Error())
			logrus.Errorf("[P2P]%s%s\n", log_pre, logmsg)
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
