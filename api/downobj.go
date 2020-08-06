package api

import (
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type DownloadObject struct {
	UClient *Client
	Length  int64
	REFS    []*pkt.Refer
	BkCall  BackupCaller
}

func (self *DownloadObject) SetBackupCaller(call BackupCaller) {
	self.BkCall = call
}

func (self *DownloadObject) InitByVHW(vhw []byte) *pkt.ErrorMessage {
	req := &pkt.DownloadObjectInitReqV2{
		UserId:    &self.UClient.UserId,
		SignData:  &self.UClient.Sign,
		KeyNumber: &self.UClient.KeyNumber,
		VHW:       vhw,
	}
	return self.init(req, base58.Encode(vhw))
}

func (self *DownloadObject) InitByKey(bucketName, filename string, version primitive.ObjectID) *pkt.ErrorMessage {
	req := &pkt.DownloadFileReqV2{
		UserId:     &self.UClient.UserId,
		SignData:   &self.UClient.Sign,
		KeyNumber:  &self.UClient.KeyNumber,
		Bucketname: &bucketName,
		FileName:   &filename,
	}
	key := "/" + bucketName + "/" + filename
	if version != primitive.NilObjectID {
		i1, i2, i3, i4 := pkt.ObjectIdParam(version)
		v := &pkt.DownloadFileReqV2_VersionId{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req.Versionid = v
		key = key + "/" + version.Hex()
	}
	return self.init(req, key)
}

func (self *DownloadObject) init(req proto.Message, key string) *pkt.ErrorMessage {
	startTime := time.Now()
	resp, errmsg := net.RequestSN(req, self.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[DownloadOBJ][%s]Init ERR:%s\n", key, pkt.ToError(errmsg))
		return errmsg
	}
	dresp, OK := resp.(*pkt.DownloadObjectInitResp)
	if OK {
		if dresp.Length == nil || dresp.Reflist == nil || dresp.Reflist.Refers == nil || len(dresp.Reflist.Refers) == 0 {
			logrus.Errorf("[DownloadOBJ][%s]Init ERR:RETURN_NULL\n", key)
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "NULL_REF")
		}
		self.Length = int64(*dresp.Length)
		refs := []*pkt.Refer{}
		for _, ref := range dresp.Reflist.Refers {
			r := pkt.NewRefer(ref)
			if r == nil {
				logrus.Errorf("[DownloadOBJ][%s]Init ERR:RETURN_NULL_REF\n", key)
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, "NULL_REF")
			}
			refs = append(refs, r)
		}
		self.REFS = refs
	}
	logrus.Infof("[DownloadOBJ][%s]Init OK, length %d,num of blocks %d,take times %d ms.\n", key, self.Length,
		len(self.REFS), time.Now().Sub(startTime).Milliseconds())
	return nil
}

func (self *DownloadObject) Load() io.Reader {
	rd := NewDownLoadReader(self, 0, self.Length)
	return rd
}

func (self *DownloadObject) LoadRange(start, end int64) io.Reader {
	rd := NewDownLoadReader(self, start, end)
	return rd
}
