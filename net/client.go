package net

import (
	"bytes"
	"context"
	"fmt"
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
	lastTime      *int64
	connectedTime *int64
	statu         *int32
	PeerId        peer.ID
	sync.Mutex
}

func NewP2P(key string) (*TcpClient, *pkt.ErrorMessage) {
	c := &TcpClient{}
	c.lastTime = new(int64)
	c.statu = new(int32)
	atomic.StoreInt32(c.statu, 0)
	c.connectedTime = new(int64)
	atomic.StoreInt64(c.connectedTime, 0)
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
	return time.Now().Unix()-atomic.LoadInt64(client.lastTime) <= env.CONN_EXPIRED
}

func (client *TcpClient) Request(msgid int32, data []byte, addrs []string, log_pre string, nowait bool) (proto.Message, *pkt.ErrorMessage) {
	if atomic.LoadInt32(client.statu) == 1 {
		addrString := AddrsToString(addrs)
		logmsg := fmt.Sprintf("[P2P]%s%s Connection destroyed!\n", log_pre, addrString)
		logrus.Errorf(logmsg)
		return nil, pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
	}
	atomic.StoreInt64(client.lastTime, time.Now().Unix())

	//输出地址
	addrString := AddrsToString(addrs)
	logmsg := fmt.Sprintf("[client] connect addrs=%s \n", addrString)
	logrus.Infof(logmsg)

	maddrs, Err := StringListToMaddrs(addrs)
	if Err != nil {
		logmsg := fmt.Sprintf("[P2P]%sAddrs %s ERR:%s\n", log_pre, addrString, Err.Error())
		logrus.Errorf(logmsg)
		return  nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, logmsg)
	}

	logrus.Printf("maddrs lenth is %d\n", len(maddrs))
	isHttp := false
	for _, maddr := range maddrs {
		 if _, err := maddr.ValueForProtocol(ma.P_HTTP); err == nil {
		 	isHttp = true
		 	logrus.Printf("maddr support HTTP \n")
		 	break
		 }
	}

	if !isHttp {
		logrus.Printf("maddr not support HTTP \n")
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
				atomic.StoreInt64(client.connectedTime, 0)
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
			return nil, errmsg
		} else {
			return msg, nil
		}
	}else {
		return  nil, pkt.NewErrorMsg(pkt.BAD_MESSAGE, "")
	}
}

func (client *TcpClient) connect(addrs []string, log_pre string, nowait bool) *pkt.ErrorMessage {
	if atomic.LoadInt64(client.connectedTime) >= 0 {
		client.Lock()
		defer client.Unlock()
		atomic.StoreInt32(client.statu, 2)
		defer atomic.StoreInt32(client.statu, 0)
		contime := atomic.LoadInt64(client.connectedTime)
		if contime >= 0 {
			addrString := AddrsToString(addrs)
			if time.Now().Unix()-contime < env.DN_RETRY_WAIT {
				logmsg := fmt.Sprintf("[P2P]%s%s did not connect successfully %d seconds ago\n", log_pre, addrString, env.DN_RETRY_WAIT)
				logrus.Errorf(logmsg)
				return pkt.NewErrorMsg(pkt.CONN_ERROR, logmsg)
			} else {
				maddrs, err := StringListToMaddrs(addrs)
				if err != nil {
					atomic.StoreInt64(client.connectedTime, time.Now().Unix())
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
					atomic.StoreInt64(client.connectedTime, time.Now().Unix())
					logmsg := fmt.Sprintf("[P2P]%sConnect %s ERR:%s\n", log_pre, addrString, err.Error())
					logrus.Errorf(logmsg)
					return pkt.NewErrorMsg(pkt.COMM_ERROR, logmsg)
				}
				atomic.StoreInt64(client.connectedTime, -1)
			}
		}
	}
	return nil
}

func (client *TcpClient) DisConnect() {
	defer env.TracePanic()
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
