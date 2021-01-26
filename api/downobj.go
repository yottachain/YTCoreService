package api

import (
	"crypto/md5"
	"errors"
	"io"
	"os"
	"strings"
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
	UClient  *Client
	Length   int64
	REFS     []*pkt.Refer
	RSS      [][]byte
	BkCall   BackupCaller
	Progress *DownProgress
}

type DownProgress struct {
	Path          string
	TotalBlockNum int32
	ReadBlockNum  int32
	Complete      bool
}

func (self *DownloadObject) SetBackupCaller(call BackupCaller) {
	self.BkCall = call
}

func (self *DownloadObject) GetProgress() int32 {
	if self.Progress.Complete {
		return 100
	}
	if self.Progress.TotalBlockNum == 0 || self.Progress.ReadBlockNum == 0 {
		return 0
	}
	return (self.Progress.ReadBlockNum - 1) * 100 / self.Progress.TotalBlockNum
}

func (self *DownloadObject) InitByVHW(vhw []byte) *pkt.ErrorMessage {
	req := &pkt.DownloadObjectInitReqV2{
		UserId:    &self.UClient.UserId,
		SignData:  &self.UClient.SignKey.Sign,
		KeyNumber: &self.UClient.SignKey.KeyNumber,
		VHW:       vhw,
	}
	return self.init(req, base58.Encode(vhw))
}

func (self *DownloadObject) InitByKey(bucketName, filename string, version primitive.ObjectID) *pkt.ErrorMessage {
	req := &pkt.DownloadFileReqV2{
		UserId:     &self.UClient.UserId,
		SignData:   &self.UClient.SignKey.Sign,
		KeyNumber:  &self.UClient.SignKey.KeyNumber,
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
	} else {
		logrus.Errorf("[DownloadOBJ][%s]Init ERR:RETURN_ERR_MSG\n", key)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
	logrus.Infof("[DownloadOBJ][%s]Init OK, length %d,num of blocks %d,take times %d ms.\n", key, self.Length,
		len(self.REFS), time.Now().Sub(startTime).Milliseconds())
	if self.Progress != nil {
		self.Progress.TotalBlockNum = int32(len(self.REFS))
	}
	return nil
}

func (self *DownloadObject) Load() io.ReadCloser {
	rd := NewDownLoadReader(self, 0, self.Length)
	return rd
}

func (self *DownloadObject) LoadRange(start, end int64) io.ReadCloser {
	rd := NewDownLoadReader(self, start, end)
	return rd
}

func (self *DownloadObject) SaveToPath(path string) error {
	_, err := self.SaveToPathV2(path)
	return err
}

func (self *DownloadObject) SaveToPathV2(path string) ([]byte, error) {
	s, err := os.Stat(path)
	if err != nil {
		if !os.IsExist(err) {
			err = os.MkdirAll(path, os.ModePerm)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else {
		if !s.IsDir() {
			return nil, errors.New("The specified path is not a directory.")
		}
	}
	self.Progress.Path = strings.ReplaceAll(path, "\\", "/")
	if !strings.HasSuffix(path, "/") {
		self.Progress.Path = path + "/"
	}
	return self.SaveToFile(self.Progress.Path + "source.dat")
}

func (self *DownloadObject) SaveToFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	read := NewDownLoadReader(self, 0, self.Length)
	readbuf := make([]byte, 8192)
	md5Digest := md5.New()
	for {
		num, err := read.Read(readbuf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if num > 0 {
			bs := readbuf[0:num]
			f.Write(bs)
			md5Digest.Write(bs)
		}
		if err != nil && err == io.EOF {
			break
		}
	}
	self.Progress.Complete = true
	return md5Digest.Sum(nil), nil
}
