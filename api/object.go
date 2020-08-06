package api

import (
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ObjectAccessor struct {
	UClient *Client
}

func (self *ObjectAccessor) CreateObject(bucketname, filename string, VNU primitive.ObjectID, meta []byte) *pkt.ErrorMessage {
	req := &pkt.UploadFileReqV2{
		UserId:     &self.UClient.UserId,
		SignData:   &self.UClient.Sign,
		KeyNumber:  &self.UClient.KeyNumber,
		Bucketname: &bucketname,
		FileName:   &filename,
		Meta:       meta,
	}
	if VNU != primitive.NilObjectID {
		i1, i2, i3, i4 := pkt.ObjectIdParam(VNU)
		v := &pkt.UploadFileReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req.Vnu = v
	} else {
		logrus.Errorf("[CreateObject][%d]%s/%s ERR:VNU is null\n", self.UClient.UserId, bucketname, filename)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "NULL_VNU")
	}
	_, errmsg := net.RequestSN(req, self.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[CreateObject][%d]%s/%s ERR:%s\n", self.UClient.UserId, bucketname, filename, pkt.ToError(errmsg))
		return errmsg
	} else {
		logrus.Infof("[CreateObject][%d]%s/%s OK.\n", self.UClient.UserId, bucketname, filename)
		return nil
	}
}
