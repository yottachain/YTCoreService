package net

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

var connects = struct {
	sync.RWMutex
	cons map[string]*TcpClient
}{cons: make(map[string]*TcpClient)}

func ClearClient() bool {
	var del_id string = ""
	var del_c *TcpClient
	connects.RLock()
	for k, v := range connects.cons {
		if !v.IsActive() {
			del_id = k
			del_c = v
			break
		}
	}
	if del_id == "" {
		connects.RUnlock()
		return true
	} else {
		connects.RUnlock()
		connects.Lock()
		delete(connects.cons, del_id)
		del_c.DisConnect()
		connects.Unlock()
		return false
	}
}

func NewClient(pid string) (*TcpClient, *pkt.ErrorMessage) {
	connects.RLock()
	con := connects.cons[pid]
	connects.RUnlock()
	var err *pkt.ErrorMessage
	if con == nil {
		connects.Lock()
		con = connects.cons[pid]
		if con == nil {
			con, err = NewP2P(pid)
			if err == nil {
				connects.cons[pid] = con
			}
		}
		connects.Unlock()
	}
	return con, nil
}

func RemoveClient(pid string) {
	connects.Lock()
	con := connects.cons[pid]
	if con != nil {
		delete(connects.cons, pid)
		con.DisConnect()
	}
	connects.Unlock()
}

type TcpClient struct {
	lastTime      *env.AtomInt64
	connectedTime *env.AtomInt64
	statu         *int32
	PeerId        peer.ID
	sync.Mutex
}

func NewP2P(key string) (*TcpClient, *pkt.ErrorMessage) {
	c := &TcpClient{}
	c.lastTime = env.NewAtomInt64(0)
	c.statu = new(int32)
	atomic.StoreInt32(c.statu, 0)
	c.connectedTime = env.NewAtomInt64(0)
	id, err := peer.Decode(key)
	if err != nil {
		logmsg := fmt.Sprintf("[P2P]PeerID %s INVALID.\n", err.Error())
		logrus.Errorf(logmsg)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, logmsg)
	}
	c.PeerId = id
	return c, nil
}

func (client *TcpClient) IsActive() bool {
	return time.Now().Unix()-client.lastTime.Value() <= env.CONN_EXPIRED
}

func (client *TcpClient) Request(msgid int32, data []byte, addrs []string, log_pre string, nowait bool) (proto.Message, *pkt.ErrorMessage) {
	if atomic.LoadInt32(client.statu) == 1 {
		addrString := AddrsToString(addrs)
		logmsg := fmt.Sprintf("%s Connection destroyed!", addrString)
		logrus.Errorf("[P2P]%s%s\n", log_pre, logmsg)
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
	}
	client.lastTime.Set(time.Now().Unix())
	err := client.connect(addrs, log_pre, nowait)
	if err != nil {
		return nil, err
	}
	timeout := time.Millisecond * time.Duration(env.Writetimeout)
	if nowait {
		timeout = time.Millisecond * time.Duration(env.DirectWritetimeout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, serr := p2phst.SendMsg(ctx, client.PeerId, msgid, data)
	if serr != nil {
		addrString := AddrsToString(addrs)
		logmsg := fmt.Sprintf("%s COMM_ERROR:%s", addrString, serr.Error())
		logrus.Errorf("[P2P]%s%s\n", log_pre, logmsg)
		if atomic.LoadInt32(client.statu) != 2 {
			client.connectedTime.Set(0)
		}
		if strings.Contains(serr.Error(), "connection is shut down") {
			p2phst.ClientStore().Close(client.PeerId)
		}
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
	}
	msg := pkt.UnmarshalMsg(res)
	if errmsg, ok := msg.(*pkt.ErrorMessage); ok {
		return nil, errmsg
	} else {
		return msg, nil
	}
}

func (client *TcpClient) RequestSN(msgid int32, data []byte, addrs []string, maddrs []ma.Multiaddr, log_pre string, nowait bool) (proto.Message, *pkt.ErrorMessage) {
	if atomic.LoadInt32(client.statu) == 1 {
		addrString := AddrsToString(addrs)
		logmsg := fmt.Sprintf("[P2P]%s%s Connection destroyed!\n", log_pre, addrString)
		logrus.Errorf(logmsg)
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
	}

	client.lastTime.Set(time.Now().Unix())

	if nil == maddrs {
		logmsg := fmt.Sprintf("[P2P]%s COMM_ERROR: maddrs is nil\n", log_pre)
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
	}

	isHttp := false
	for _, maddr := range maddrs {
		if _, err := maddr.ValueForProtocol(ma.P_HTTP); err == nil {
			isHttp = true
			logrus.Debugf("maddr support HTTP \n")
			break
		}
	}

	if !isHttp {
		logrus.Debugf("maddr not support HTTP \n")
		err := client.connect(addrs, log_pre, nowait)
		if err != nil {
			return nil, err
		}
	}

	timeout := time.Millisecond * time.Duration(env.Writetimeout)
	if nowait {
		timeout = time.Millisecond * time.Duration(env.DirectWritetimeout)
	}
	var msg proto.Message
	var sSuc = false
	for _, maddr := range maddrs {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		res, serr := p2phst.SendMsgAuto(ctx, client.PeerId, msgid, maddr, data)
		if serr != nil {
			addrString := AddrsToString(addrs)
			logmsg := fmt.Sprintf("[P2P]%s%s COMM_ERROR:%s\n", log_pre, addrString, serr.Error())
			logrus.Errorf(logmsg)
			if atomic.LoadInt32(client.statu) != 2 {
				client.connectedTime.Set(0)
			}
			cancel()
			continue
		}

		sSuc = true
		msg = pkt.UnmarshalMsg(res)
		break
	}

	if sSuc {
		if errmsg, ok := msg.(*pkt.ErrorMessage); ok {
			addrString := AddrsToString(addrs)
			logrus.Errorf("%s%s return msg error %s data hex=%x\n", log_pre, addrString, errmsg.Msg, data)
			return nil, errmsg
		} else {
			return msg, nil
		}
	} else {
		logmsg := fmt.Sprintf("[P2P]%s COMM_ERROR: all send fail\n", log_pre)
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
	}
}

func (client *TcpClient) connect(addrs []string, log_pre string, nowait bool) *pkt.ErrorMessage {
	if client.connectedTime.Value() >= 0 {
		client.Lock()
		defer client.Unlock()
		atomic.StoreInt32(client.statu, 2)
		defer atomic.StoreInt32(client.statu, 0)
		contime := client.connectedTime.Value()
		if contime >= 0 {
			addrString := AddrsToString(addrs)
			if time.Now().Unix()-contime < env.DN_RETRY_WAIT {
				logmsg := fmt.Sprintf("[P2P]%s%s did not connect successfully %d seconds ago\n", log_pre, addrString, env.DN_RETRY_WAIT)
				logrus.Errorf(logmsg)
				return pkt.NewErrorMsg(pkt.CONN_ERROR, logmsg)
			} else {
				maddrs, err := StringListToMaddrs(addrs)
				if err != nil {
					client.connectedTime.Set(time.Now().Unix())
					logmsg := fmt.Sprintf("[P2P]%sAddrs %s ERR:%s\n", log_pre, addrString, err.Error())
					logrus.Errorf(logmsg)
					return pkt.NewErrorMsg(pkt.INVALID_ARGS, logmsg)
				}
				timeout := time.Millisecond * time.Duration(env.Conntimeout)
				if nowait {
					timeout = time.Millisecond * time.Duration(env.DirectConntimeout)
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				_, err = p2phst.ClientStore().Get(ctx, client.PeerId, maddrs)
				if err != nil {
					client.connectedTime.Set(time.Now().Unix())
					logmsg := fmt.Sprintf("[P2P]%sConnect %s ERR:%s\n", log_pre, addrString, err.Error())
					logrus.Errorf(logmsg)
					return pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
				}
				client.connectedTime.Set(-1)
			}
		}
	}
	return nil
}

func (client *TcpClient) DisConnect() {
	atomic.StoreInt32(client.statu, 1)
	p2phst.ClientStore().Close(client.PeerId)
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
