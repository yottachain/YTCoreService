package net

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	cli "github.com/yottachain/YTHost/client"
)

type Node struct {
	Id     int32
	Nodeid string
	Addrs  []string
	Pubkey string
	Weight float64

	PeerId peer.ID
	Maddr  []ma.Multiaddr
}

func (n *Node) Init() error {
	pid, err := peer.Decode(n.Nodeid)
	if err != nil {
		return err
	}
	n.PeerId = pid
	ma, err := StringListToMaddrs(n.Addrs)
	if err != nil {
		return err
	}
	n.Maddr = ma
	return nil
}

func (n *Node) Addstrings() string {
	return AddrsToString(n.Addrs)
}

func DoRequest(msg proto.Message, PeerId peer.ID, Maddr []ma.Multiaddr) (proto.Message, *pkt.ErrorMessage) {
	data, _, msgtype, merr := pkt.MarshalMsg(msg)
	if merr != nil {
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, merr.Error())
	}
	var client *cli.YTHostClient
	if c, ok := p2phst.ClientStore().GetClient(PeerId); !ok {
		newc, err := p2phst.ClientStore().Get(context.Background(), PeerId, Maddr)
		if err != nil {
			return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, fmt.Sprintf("%s,occurred on %s", err.Error(), MultiAddrsToString(Maddr)))
		}
		client = newc
	} else {
		client = c
	}
	res, err := client.SendMsg(context.Background(), msgtype, data)
	if err != nil {
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, fmt.Sprintf("%s,occurred on %s", err.Error(), MultiAddrsToString(Maddr)))
	}
	resmsg := pkt.UnmarshalMsg(res)
	if errmsg, ok := msg.(*pkt.ErrorMessage); ok {
		return nil, errmsg
	} else {
		return resmsg, nil
	}
}

func RequestDN(msg proto.Message, dn *Node) (proto.Message, *pkt.ErrorMessage) {
	return DoRequest(msg, dn.PeerId, dn.Maddr)
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
			resmsg, errmsg = DoRequest(msg, snclient.PeerId, snclient.TcpAddr)
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

func StringListToMaddrs(addrs []string) ([]ma.Multiaddr, error) {
	maddrs := make([]ma.Multiaddr, len(addrs))
	for k, addr := range addrs {
		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			return maddrs, err
		}
		maddrs[k] = maddr
	}
	return maddrs, nil
}

func MultiAddrsToString(addrs []ma.Multiaddr) string {
	defer env.TracePanic("[P2P]")
	var buffer bytes.Buffer
	for index, addr := range addrs {
		if index == 0 {
			buffer.WriteString("[")
			buffer.WriteString(addr.String())
		} else {
			buffer.WriteString(",")
			buffer.WriteString(addr.String())
		}
	}
	if buffer.Len() > 0 {
		buffer.WriteString("]")
	}
	return buffer.String()
}

func AddrsToString(addrs []string) string {
	var buffer bytes.Buffer
	for index, addr := range addrs {
		if index == 0 {
			buffer.WriteString("[")
			buffer.WriteString(addr)
		} else {
			buffer.WriteString(",")
			buffer.WriteString(addr)
		}
	}
	if buffer.Len() > 0 {
		buffer.WriteString("]")
	}
	return buffer.String()
}
