package net

import (
	"bufio"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTCoreService/env"
)

type GobClientCodec struct {
	rwc        io.ReadWriteCloser
	dec        *gob.Decoder
	enc        *gob.Encoder
	encBuf     *bufio.Writer
	ActiveTime *int64
}

func (c *GobClientCodec) WriteRequest(r *rpc.Request, body interface{}) (err error) {
	if err = c.enc.Encode(r); err != nil {
		return
	}
	if err = c.enc.Encode(body); err != nil {
		return
	}
	err = c.encBuf.Flush()
	if err == nil {
		atomic.StoreInt64(c.ActiveTime, time.Now().Unix())
	}
	return err
}

func (c *GobClientCodec) ReadResponseHeader(r *rpc.Response) error {
	err := c.dec.Decode(r)
	if err == nil {
		atomic.StoreInt64(c.ActiveTime, time.Now().Unix())
	}
	return err
}

func (c *GobClientCodec) ReadResponseBody(body interface{}) error {
	err := c.dec.Decode(body)
	if err == nil {
		atomic.StoreInt64(c.ActiveTime, time.Now().Unix())
	}
	return err
}

func (c *GobClientCodec) Close() error {
	return c.rwc.Close()
}

type YTCall struct {
	client    *TcpClient
	args      interface{}
	reply     interface{}
	writeDone chan *rpc.Call
	cancel    int32
	call      *rpc.Call
}

func (ytcall *YTCall) WriteDone(ctx context.Context) error {
	select {
	case ytcall.call = <-ytcall.writeDone:
		return nil
	case <-ctx.Done():
		atomic.AddInt32(&ytcall.cancel, 1)
		return fmt.Errorf("ctx time out:writing")
	}
}

func (ytcall *YTCall) ReadDone(ctx context.Context) ([]byte, error) {
	if ytcall.call == nil {
		return nil, fmt.Errorf("message not sent")
	}
	select {
	case <-ytcall.call.Done:
		if ytcall.call.Error != nil {
			if ytcall.call.Error == rpc.ErrShutdown || ytcall.call.Error == io.ErrUnexpectedEOF {
				ytcall.client.Close()
			}
			return nil, ytcall.call.Error
		} else {
			return ytcall.call.Reply.(*Response).Data, nil
		}
	case <-ctx.Done():
		return nil, fmt.Errorf("ctx time out:waiting for response")
	}
}

type TcpClient struct {
	localPeerID     peer.ID
	localPeerAddrs  []string
	localPeerPubKey []byte
	Version         int32
	RPI             *PeerInfo

	reqQueue   chan *YTCall
	respQueue  chan int32
	Remover    func()
	activeTime *int64

	codec rpc.ClientCodec

	request rpc.Request

	mutex   sync.Mutex
	seq     uint64
	pending map[uint64]*rpc.Call
	closing bool
}

func NewClient(conn io.ReadWriteCloser, pi *peer.AddrInfo, pk crypto.PubKey, v int32) *TcpClient {
	yc := &TcpClient{
		localPeerID: pi.ID,
		Version:     v,
		reqQueue:    make(chan *YTCall, env.P2P_RequestQueueSize),
		respQueue:   make(chan int32, env.P2P_ResponseQueueSize),
		activeTime:  new(int64),
	}
	encBuf := bufio.NewWriter(conn)
	yc.codec = &GobClientCodec{conn, gob.NewDecoder(conn), gob.NewEncoder(encBuf), encBuf, yc.activeTime}
	yc.pending = make(map[uint64]*rpc.Call)
	yc.localPeerPubKey, _ = pk.Raw()
	for _, v := range pi.Addrs {
		yc.localPeerAddrs = append(yc.localPeerAddrs, v.String())
	}
	return yc
}

func (yc *TcpClient) Start(remover func()) {
	yc.Remover = remover
	go yc.output()
	go yc.input()
}

func (yc *TcpClient) output() {
	lasttime := time.Now()
	timer := time.NewTimer(time.Millisecond * time.Duration(env.P2P_WriteTimeout))
	for {
		select {
		case req := <-yc.reqQueue:
			if atomic.LoadInt32(&req.cancel) > 0 {
				break
			}
			if yc.IsClosed() {
				call := &rpc.Call{ServiceMethod: "ms.HandleMsg", Args: req.args, Reply: req.reply, Error: rpc.ErrShutdown, Done: make(chan *rpc.Call, 1)}
				call.Done <- call
				req.writeDone <- call
				return
			}
			req.writeDone <- yc.send(req, "ms.HandleMsg")
			lasttime = time.Now()
		case <-timer.C:
			if yc.IsClosed() || yc.IsDazed() || time.Since(lasttime).Milliseconds() > int64(env.P2P_IdleTimeout) {
				yc.Close()
				return
			}
			yc.send(&YTCall{args: "ping", reply: new(string), writeDone: make(chan *rpc.Call, 1), client: yc}, "ms.Ping")
		}
		timer.Reset(time.Millisecond * time.Duration(env.P2P_WriteTimeout))
	}
}

func (yc *TcpClient) send(req *YTCall, method string) *rpc.Call {
	call := &rpc.Call{ServiceMethod: method, Args: req.args, Reply: req.reply, Done: make(chan *rpc.Call, 1)}
	yc.respQueue <- 1
	if atomic.LoadInt32(&req.cancel) > 0 {
		<-yc.respQueue
		call.Error = fmt.Errorf("ctx time out:writing")
		call.Done <- call
		return call
	}
	yc.mutex.Lock()
	seq := yc.seq
	yc.seq++
	yc.pending[seq] = call
	yc.mutex.Unlock()
	yc.request.Seq = seq
	yc.request.ServiceMethod = call.ServiceMethod
	err := yc.codec.WriteRequest(&yc.request, call.Args)
	if err != nil {
		yc.mutex.Lock()
		errcall := yc.pending[seq]
		delete(yc.pending, seq)
		yc.mutex.Unlock()
		if errcall != nil {
			<-yc.respQueue
			errcall.Error = err
			errcall.Done <- call
		}
	}
	return call
}

func (yc *TcpClient) input() {
	var err error
	var response rpc.Response
	for err == nil {
		response = rpc.Response{}
		err = yc.codec.ReadResponseHeader(&response)
		if err != nil {
			break
		}
		seq := response.Seq
		yc.mutex.Lock()
		call := yc.pending[seq]
		delete(yc.pending, seq)
		yc.mutex.Unlock()
		if call != nil {
			<-yc.respQueue
		}
		switch {
		case call == nil:
			err = yc.codec.ReadResponseBody(nil)
			if err != nil {
				err = errors.New("reading error body: " + err.Error())
			}
		case response.Error != "":
			call.Error = rpc.ServerError(response.Error)
			err = yc.codec.ReadResponseBody(nil)
			if err != nil {
				err = errors.New("reading error body: " + err.Error())
			}
			call.Done <- call
		default:
			err = yc.codec.ReadResponseBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.Done <- call
		}
	}
	yc.mutex.Lock()
	closing := yc.closing
	if err == io.EOF {
		if closing {
			err = rpc.ErrShutdown
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	for _, call := range yc.pending {
		call.Error = err
		call.Done <- call
		<-yc.respQueue
	}
	yc.mutex.Unlock()
}

func (yc *TcpClient) IsDazed() bool {
	rwt := atomic.LoadInt64(yc.activeTime)
	if rwt > 0 && (time.Now().Unix()-rwt)*1000 > int64(env.P2P_MuteTimeout) {
		return true
	}
	return false
}

func (yc *TcpClient) pushMsg(ctx context.Context, id int32, data []byte) (*YTCall, error) {
	req := Request{MsgId: id,
		ReqData: data,
		RemotePeerInfo: PeerInfo{ID: yc.localPeerID,
			Addrs:   yc.localPeerAddrs,
			PubKey:  yc.localPeerPubKey,
			Version: yc.Version},
	}
	msg := &YTCall{args: req,
		reply:     new(Response),
		writeDone: make(chan *rpc.Call, 1),
		cancel:    0,
		client:    yc,
	}
	select {
	case yc.reqQueue <- msg:
		return msg, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("ctx time out:waiting to write")
	}
}

func (yc *TcpClient) SendMsg(ctx context.Context, id int32, data []byte) ([]byte, error) {
	if ctx == context.Background() || ctx == context.TODO() {
		writeCtx, writeCancel := context.WithTimeout(context.Background(), time.Duration(env.P2P_WriteTimeout)*time.Millisecond)
		defer writeCancel()
		ytcall, err := yc.pushMsg(writeCtx, id, data)
		if err != nil {
			return nil, err
		}
		err = ytcall.WriteDone(writeCtx)
		if err != nil {
			return nil, err
		}
		readCtx, readCancel := context.WithTimeout(context.Background(), time.Duration(env.P2P_ReadTimeout)*time.Millisecond)
		defer readCancel()
		return ytcall.ReadDone(readCtx)
	} else {
		t, isdead := ctx.Deadline()
		if isdead {
			return nil, fmt.Errorf("ctx time out:waiting to write")
		}
		deadtime := time.Until(t)
		ytcall, err := yc.pushMsg(ctx, id, data)
		if err != nil {
			return nil, err
		}
		err = ytcall.WriteDone(ctx)
		if err != nil {
			return nil, err
		}
		readCtx, readCancel := context.WithTimeout(context.Background(), deadtime)
		defer readCancel()
		return ytcall.ReadDone(readCtx)
	}
}

func (yc *TcpClient) Close() error {
	yc.mutex.Lock()
	if yc.closing {
		yc.mutex.Unlock()
		return rpc.ErrShutdown
	}
	yc.closing = true
	yc.mutex.Unlock()
	if yc.Remover != nil {
		yc.Remover()
	}
	return yc.codec.Close()
}

func (yc *TcpClient) IsClosed() bool {
	yc.mutex.Lock()
	defer yc.mutex.Unlock()
	return yc.closing
}

func (yc *TcpClient) RemotePeerInfo() (*PeerInfo, error) {
	info := new(PeerInfo)
	yc.mutex.Lock()
	defer yc.mutex.Unlock()
	if yc.RPI == nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(env.P2P_ConnectTimeout)*time.Millisecond)
		defer cancel()
		infcall := yc.send(&YTCall{args: "", reply: info, writeDone: make(chan *rpc.Call, 1), client: yc}, "as.RemotePeerInfo")
		select {
		case <-infcall.Done:
			if infcall.Error != nil {
				return nil, infcall.Error
			} else {
				yc.RPI = info
				return yc.RPI, nil
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("ctx time out:getRemotePeerInfo")
		}
	} else {
		return yc.RPI, nil
	}
}

func (yc *TcpClient) RemotePeer() peer.AddrInfo {
	var ai peer.AddrInfo
	if info, err := yc.RemotePeerInfo(); err == nil {
		ai.ID = info.ID
		for _, addr := range info.Addrs {
			ma, _ := multiaddr.NewMultiaddr(addr)
			ai.Addrs = append(ai.Addrs, ma)
		}
	}
	return ai
}

func (yc *TcpClient) RemotePeerPubkey() crypto.PubKey {
	if info, err := yc.RemotePeerInfo(); err == nil {
		if pk, er := crypto.UnmarshalPublicKey(info.PubKey); er == nil {
			return pk
		}
	}
	return nil
}

func (yc *TcpClient) RemotePeerVersion() int32 {
	if info, err := yc.RemotePeerInfo(); err == nil {
		return info.Version
	}
	return 0
}

func (yc *TcpClient) LocalPeer() peer.AddrInfo {
	pi := peer.AddrInfo{}
	pi.ID = yc.localPeerID
	for _, v := range yc.localPeerAddrs {
		ma, _ := multiaddr.NewMultiaddr(v)
		pi.Addrs = append(pi.Addrs, ma)
	}
	return pi
}
