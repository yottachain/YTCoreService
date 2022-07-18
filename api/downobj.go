package api

import (
	"crypto/md5"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mr-tron/base58/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/proto"
)

type DownloadObject struct {
	UClient  *Client
	Length   int64
	REFS     []*pkt.Refer
	RSS      [][]byte
	BkCall   BackupCaller
	Progress *DownProgress
	Meta     []byte
	VHW      []byte
}

type DownProgress struct {
	Path          string
	TotalBlockNum int32
	ReadBlockNum  int32
	Complete      bool
}

func (down *DownloadObject) SetBackupCaller(call BackupCaller) {
	down.BkCall = call
}

func (down *DownloadObject) GetTime() time.Time {
	if len(down.REFS) > 0 {
		id := down.REFS[len(down.REFS)-1].VBI
		return time.Unix(id>>32, 0)
	}
	return time.Now()
}

func (down *DownloadObject) GetProgress() int32 {
	if down.Progress.Complete {
		return 100
	}
	if down.Progress.TotalBlockNum == 0 || down.Progress.ReadBlockNum == 0 {
		return 0
	}
	return (down.Progress.ReadBlockNum - 1) * 100 / down.Progress.TotalBlockNum
}

func (down *DownloadObject) InitByVHW(vhw []byte) *pkt.ErrorMessage {
	req := &pkt.DownloadObjectInitReqV2{
		UserId:    &down.UClient.UserId,
		SignData:  &down.UClient.SignKey.Sign,
		KeyNumber: &down.UClient.SignKey.KeyNumber,
		VHW:       vhw,
	}
	return down.init(req, base58.Encode(vhw))
}

func (down *DownloadObject) InitByKey(bucketName, filename string, version primitive.ObjectID) *pkt.ErrorMessage {
	req := &pkt.GetFileAuthReq{
		UserId:     &down.UClient.UserId,
		SignData:   &down.UClient.SignKey.Sign,
		KeyNumber:  &down.UClient.SignKey.KeyNumber,
		Bucketname: &bucketName,
		FileName:   &filename,
	}
	key := "/" + bucketName + "/" + filename
	if version != primitive.NilObjectID {
		i1, i2, i3, i4 := pkt.ObjectIdParam(version)
		v := &pkt.GetFileAuthReq_VersionId{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req.Versionid = v
		key = key + "/" + version.Hex()
	}
	return down.init(req, key)
}

func (down *DownloadObject) init(req proto.Message, key string) *pkt.ErrorMessage {
	startTime := time.Now()
	resp, errmsg := net.RequestSN(req)
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
		down.Length = int64(*dresp.Length)
		refs := []*pkt.Refer{}
		for _, ref := range dresp.Reflist.Refers {
			r := pkt.NewRefer(ref)
			if r == nil {
				logrus.Errorf("[DownloadOBJ][%s]Init ERR:RETURN_NULL_REF\n", key)
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, "NULL_REF")
			}
			refs = append(refs, r)
		}
		down.REFS = refs
	} else {
		presp, OK := resp.(*pkt.GetFileAuthResp)
		if OK {
			if presp.Length == nil || presp.Reflist == nil || presp.Reflist.Refers == nil || len(presp.Reflist.Refers) == 0 {
				logrus.Errorf("[DownloadOBJ][%s]Init ERR:RETURN_NULL\n", key)
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, "NULL_REF")
			}
			down.VHW = presp.VHW
			down.Meta = presp.Meta
			down.Length = int64(*presp.Length)
			refs := []*pkt.Refer{}
			for _, ref := range presp.Reflist.Refers {
				r := pkt.NewRefer(ref)
				if r == nil {
					logrus.Errorf("[DownloadOBJ][%s]Init ERR:RETURN_NULL_REF\n", key)
					return pkt.NewErrorMsg(pkt.SERVER_ERROR, "NULL_REF")
				}
				refs = append(refs, r)
			}
			down.REFS = refs
		} else {
			logrus.Errorf("[DownloadOBJ][%s]Init ERR:RETURN_ERR_MSG\n", key)
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
		}
	}
	logrus.Infof("[DownloadOBJ][%s]Init OK, length %d,num of blocks %d,take times %d ms.\n", key, down.Length,
		len(down.REFS), time.Since(startTime).Milliseconds())
	if down.Progress != nil {
		down.Progress.TotalBlockNum = int32(len(down.REFS))
	}
	return nil
}

func (down *DownloadObject) Load() io.ReadCloser {
	rd := NewDownLoadReader(down, 0, down.Length)
	return rd
}

func (down *DownloadObject) LoadRange(start, end int64) io.ReadCloser {
	rd := NewDownLoadReader(down, start, end)
	return rd
}

func (down *DownloadObject) SaveToPath(path string) error {
	_, err := down.SaveToPathV2(path)
	return err
}

func (down *DownloadObject) SaveToPathV2(path string) ([]byte, error) {
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
			return nil, errors.New("the specified path is not a directory")
		}
	}
	down.Progress.Path = strings.ReplaceAll(path, "\\", "/")
	if !strings.HasSuffix(path, "/") {
		down.Progress.Path = path + "/"
	}
	return down.SaveToFile(down.Progress.Path + "source.dat")
}

func (down *DownloadObject) SaveToFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	read := NewDownLoadReader(down, 0, down.Length)
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
	down.Progress.Complete = true
	return md5Digest.Sum(nil), nil
}
