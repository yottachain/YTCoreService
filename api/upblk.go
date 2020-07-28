package api

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTDNMgmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var BLOCK_ROUTINE_CH chan int

func InitBlockRoutinePool() {
	BLOCK_ROUTINE_CH = make(chan int, env.UploadBlockThreadNum)
	for ii := 0; ii < env.UploadBlockThreadNum; ii++ {
		BLOCK_ROUTINE_CH <- 1
	}
}

func StartUploadBlock(cl *Client, id int16, b *codec.PlainBlock, v primitive.ObjectID, sign string, stamp int64) {
	AddBlockMen(&b.Block)
	ub := &UploadBlock{UClient: cl, ID: id, Sign: sign, Stamp: stamp, VNU: v, BLK: b}
	ub.logPrefix = fmt.Sprintf("[%s][%d]", ub.VNU.Hex(), ub.ID)
	<-BLOCK_ROUTINE_CH
	go ub.upload()
}

type UploadBlock struct {
	ID        int16
	Sign      string
	Stamp     int64
	BLK       *codec.PlainBlock
	VNU       primitive.ObjectID
	Queue     *DNQueue
	STime     int64
	logPrefix string
	ERR       error
	SN        *YTDNMgmt.SuperNode
	UClient   *Client
}

func (self *UploadBlock) DoFinish() {
	BLOCK_ROUTINE_CH <- 1
	DecBlockMen(&self.BLK.Block)
	if self.Queue != nil {
		self.Queue.Close()
	}
	if r := recover(); r != nil {
		logrus.Tracef("%sERR:%s\n", self.logPrefix, r)
	}
}

func (self *UploadBlock) upload() {
	defer self.DoFinish()
	err := self.BLK.Sum()
	if err != nil {
		self.ERR = err
		return
	}
	self.SN = net.GetBlockSuperNode(self.BLK.VHP)
	logrus.Infof("%sStart upload block to sn %d\n", self.logPrefix, self.SN.ID)
	_ = pkt.UploadBlockInitReqV2{UserId: &self.UClient.UserId, SignData: &self.UClient.Sign, KeyNumber: &self.UClient.KeyNumber}

}
