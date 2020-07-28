package api

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var BLOCK_ROUTINE_CH chan int

func InitBlockRoutinePool() {
	BLOCK_ROUTINE_CH = make(chan int, env.UploadBlockThreadNum)
	for ii := 0; ii < env.UploadBlockThreadNum; ii++ {
		BLOCK_ROUTINE_CH <- 1
	}
}

func StartUploadBlock(id int16, b *codec.Block, v primitive.ObjectID, sign string, stamp int64) {
	ub := &UploadBlock{ID: id, Sign: sign, Stamp: stamp, VNU: v, BLK: b}
	ub.logPrefix = fmt.Sprintf("[%s][%d]", ub.VNU.Hex(), ub.ID)
	<-BLOCK_ROUTINE_CH
	go ub.upload()
}

type UploadBlock struct {
	ID        int16
	Sign      string
	Stamp     int64
	BLK       *codec.Block
	VNU       primitive.ObjectID
	Queue     *DNQueue
	STime     int64
	logPrefix string
}

func (self *UploadBlock) DoFinish() {
	BLOCK_ROUTINE_CH <- 1
	if r := recover(); r != nil {
		logrus.Tracef("%sERR:%s\n", self.logPrefix, r)
	}
}

func (self *UploadBlock) upload() {

}
