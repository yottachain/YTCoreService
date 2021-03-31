package api

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ObjectAccessor struct {
	UClient *Client
}

type FileItem struct {
	FileId    primitive.ObjectID
	BucketId  primitive.ObjectID
	FileName  string
	VersionId primitive.ObjectID
	Latest    bool
	Meta      []byte
	Acl       []byte
}

const LengthKey = "contentLength"
const ETagKey = "ETag"
const DateKey = "x-amz-date"

func BytesToFileMetaMap(meta []byte, versionid primitive.ObjectID) (map[string]string, error) {
	if meta == nil {
		return nil, errors.New("no data")
	}
	if len(meta) >= 24 {
		m, err := pkt.UnmarshalMap(meta)
		if err == nil {
			s, _ := m[ETagKey]
			if s != "" {
				return m, nil
			}
		}
		id := env.BytesToId(meta)
		hash := meta[8:]
		m = make(map[string]string)
		m[LengthKey] = strconv.Itoa(int(id))
		m[ETagKey] = hex.EncodeToString(hash)
		if versionid != primitive.NilObjectID {
			m[DateKey] = versionid.Timestamp().String()
		}
		return m, nil
	} else {
		return nil, errors.New("err data")
	}
}

func MetaTobytes(length int64, md5 []byte) []byte {
	bs1 := env.IdToBytes(length)
	return bytes.Join([][]byte{bs1, md5}, []byte{})
}

func FileMetaMapTobytes(m map[string]string) ([]byte, error) {
	s1, _ := m[ETagKey]
	s1 = strings.ReplaceAll(s1, "\\", "")
	bs2, err := hex.DecodeString(s1)
	if err != nil {
		return nil, errors.New(ETagKey + " value err.")
	}
	if len(bs2) != 16 {
		return nil, errors.New(ETagKey + " value err.")
	}
	s2, _ := m[LengthKey]
	size, err := strconv.Atoi(s2)
	if err != nil {
		return nil, errors.New(LengthKey + " value err.")
	}
	bs1 := env.IdToBytes(int64(size))
	return bytes.Join([][]byte{bs1, bs2}, []byte{}), nil
}

func (self *ObjectAccessor) CreateObject(bucketname, filename string, VNU primitive.ObjectID, meta []byte) *pkt.ErrorMessage {
	req := &pkt.UploadFileReqV2{
		UserId:     &self.UClient.UserId,
		SignData:   &self.UClient.SignKey.Sign,
		KeyNumber:  &self.UClient.SignKey.KeyNumber,
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

func (self *ObjectAccessor) CopyObject(srcbuck, srckey, destbuck, destkey string) (*FileItem, *pkt.ErrorMessage) {
	req := &pkt.CopyObjectReqV2{
		UserId:        &self.UClient.UserId,
		SignData:      &self.UClient.SignKey.Sign,
		KeyNumber:     &self.UClient.SignKey.KeyNumber,
		SrcBucket:     &srcbuck,
		SrcObjectKey:  &srckey,
		DestBucket:    &destbuck,
		DestObjectKey: &destkey,
	}
	resp, errmsg := net.RequestSN(req, self.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[CopyObject][%d]%s/%s-->%s/%s ERR:%s\n", self.UClient.UserId, srcbuck, srckey, destbuck, destkey, pkt.ToError(errmsg))
		return nil, errmsg
	}
	dresp, OK := resp.(*pkt.CopyObjectResp)
	if OK {
		if dresp.Versionid == nil || dresp.Versionid.Timestamp == nil || dresp.Versionid.MachineIdentifier == nil || dresp.Versionid.ProcessIdentifier == nil || dresp.Versionid.Counter == nil {
			logrus.Errorf("[CopyObject][%d]%s/%s-->%s/%s return NULL Versionid\n", self.UClient.UserId, srcbuck, srckey, destbuck, destkey)
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Version Return Nil")
		}
		if dresp.Bucketid == nil || dresp.Bucketid.Timestamp == nil || dresp.Bucketid.MachineIdentifier == nil || dresp.Bucketid.ProcessIdentifier == nil || dresp.Bucketid.Counter == nil {
			logrus.Errorf("[CopyObject][%d]%s/%s-->%s/%s return NULL Bucketid\n", self.UClient.UserId, srcbuck, srckey, destbuck, destkey)
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Bucketid Return Nil")
		}
		if dresp.FileName == nil {
			logrus.Errorf("[CopyObject][%d]%s/%s-->%s/%s return NULL FileName\n", self.UClient.UserId, srcbuck, srckey, destbuck, destkey)
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "FileName Return Nil")
		}
		item := &FileItem{FileName: *dresp.FileName, Meta: dresp.Meta}
		item.BucketId = pkt.NewObjectId(*dresp.Bucketid.Timestamp, *dresp.Bucketid.MachineIdentifier, *dresp.Bucketid.ProcessIdentifier, *dresp.Bucketid.Counter)
		item.VersionId = pkt.NewObjectId(*dresp.Versionid.Timestamp, *dresp.Versionid.MachineIdentifier, *dresp.Versionid.ProcessIdentifier, *dresp.Versionid.Counter)
		logrus.Infof("[CopyObject][%d]%s/%s-->%s/%s OK.\n", self.UClient.UserId, srcbuck, srckey, destbuck, destkey)
		return item, nil
	} else {
		logrus.Errorf("[CopyObject][%d]%s/%s-->%s/%s return err msg type.\n", self.UClient.UserId, srcbuck, srckey, destbuck, destkey)
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
}

var List_Compress bool = true

func (self *ObjectAccessor) ListObject(buck, fileName, prefix string, wversion bool, nVerid primitive.ObjectID, limit uint32) ([]*FileItem, *pkt.ErrorMessage) {
	req := &pkt.ListObjectReqV2{
		UserId:     &self.UClient.UserId,
		SignData:   &self.UClient.SignKey.Sign,
		KeyNumber:  &self.UClient.SignKey.KeyNumber,
		BucketName: &buck,
		FileName:   &fileName,
		Prefix:     &prefix,
		Limit:      &limit,
		Compress:   &List_Compress,
	}
	if wversion {
		i1, i2, i3, i4 := pkt.ObjectIdParam(nVerid)
		v := &pkt.ListObjectReqV2_NextVersionId{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req.Nextversionid = v
	}
	resp, errmsg := net.RequestSN(req, self.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[ListObject][%d]%s/%s/%s/%s ERR:%s\n", self.UClient.UserId, buck, fileName, prefix, nVerid.Hex(), pkt.ToError(errmsg))
		return nil, errmsg
	}
	respv1, OK1 := resp.(*pkt.ListObjectResp)
	if OK1 {
		ret, errmsg := self.GetListRespV1(respv1)
		if errmsg != nil {
			logrus.Errorf("[ListObject][%d]%s/%s/%s/%s ERR:%s\n", self.UClient.UserId, buck, fileName, prefix, nVerid.Hex(), pkt.ToError(errmsg))
			return nil, errmsg
		} else {
			logrus.Infof("[ListObject][%d]%s/%s/%s/%s OK,count:%d\n", self.UClient.UserId, buck, fileName, prefix, nVerid.Hex(), len(ret))
			return ret, nil
		}
	}
	respv2, OK2 := resp.(*pkt.ListObjectRespV2)
	if OK2 {
		ret, errmsg := self.GetListRespV2(respv2)
		if errmsg != nil {
			logrus.Errorf("[ListObject][%d]%s/%s/%s/%s ERR:%s\n", self.UClient.UserId, buck, fileName, prefix, nVerid.Hex(), pkt.ToError(errmsg))
			return nil, errmsg
		} else {
			logrus.Infof("[ListObject][%d]%s/%s/%s/%s OK,count:%d\n", self.UClient.UserId, buck, fileName, prefix, nVerid.Hex(), len(ret))
			return ret, nil
		}
	}
	logrus.Errorf("[ListObject][%d]%s/%s/%s/%s return err msg type.\n", self.UClient.UserId, buck, fileName, prefix, nVerid.Hex())
	return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
}

func (self *ObjectAccessor) GetListRespV1(resp *pkt.ListObjectResp) ([]*FileItem, *pkt.ErrorMessage) {
	items := []*FileItem{}
	if resp.Filemetalist == nil {
		return items, nil
	}
	for _, item := range resp.Filemetalist {
		m := &FileItem{Meta: item.Meta, Acl: item.Acl, Latest: true}
		if item.Fileid == nil || item.Fileid.Timestamp == nil || item.Fileid.MachineIdentifier == nil || item.Fileid.ProcessIdentifier == nil || item.Fileid.Counter == nil {
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Fileid Return Nil")
		}
		m.FileId = pkt.NewObjectId(*item.Fileid.Timestamp, *item.Fileid.MachineIdentifier, *item.Fileid.ProcessIdentifier, *item.Fileid.Counter)
		if item.Bucketid == nil || item.Bucketid.Timestamp == nil || item.Bucketid.MachineIdentifier == nil || item.Bucketid.ProcessIdentifier == nil || item.Bucketid.Counter == nil {
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Bucketid Return Nil")
		}
		m.BucketId = pkt.NewObjectId(*item.Bucketid.Timestamp, *item.Bucketid.MachineIdentifier, *item.Bucketid.ProcessIdentifier, *item.Bucketid.Counter)
		if item.Versionid == nil || item.Versionid.Timestamp == nil || item.Versionid.MachineIdentifier == nil || item.Versionid.ProcessIdentifier == nil || item.Versionid.Counter == nil {
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Versionid Return Nil")
		}
		m.VersionId = pkt.NewObjectId(*item.Versionid.Timestamp, *item.Versionid.MachineIdentifier, *item.Versionid.ProcessIdentifier, *item.Versionid.Counter)
		if item.FileName == nil {
			return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "FileName Return Nil")
		}
		m.FileName = *item.FileName
		if item.Latest != nil {
			m.Latest = *item.Latest
		}
		items = append(items, m)
	}
	return items, nil
}

func (self *ObjectAccessor) GetListRespV2(resp *pkt.ListObjectRespV2) ([]*FileItem, *pkt.ErrorMessage) {
	if resp.Data == nil || len(resp.Data) == 0 {
		return []*FileItem{}, nil
	}
	msg := &pkt.ListObjectResp{}
	data := resp.Data
	rd, err := gzip.NewReader(bytes.NewReader(data))
	if err == nil {
		d, err := ioutil.ReadAll(rd)
		if err == nil {
			data = d
		}
	}
	err = proto.Unmarshal(data, msg)
	if err != nil {
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return ListObjectRespV2 ERR")
	}
	return self.GetListRespV1(msg)
}

func (self *ObjectAccessor) DeleteObject(buck, fileName string, Verid primitive.ObjectID) *pkt.ErrorMessage {
	req := &pkt.DeleteFileReqV2{
		UserId:     &self.UClient.UserId,
		SignData:   &self.UClient.SignKey.Sign,
		KeyNumber:  &self.UClient.SignKey.KeyNumber,
		BucketName: &buck,
		FileName:   &fileName,
	}
	if Verid != primitive.NilObjectID {
		i1, i2, i3, i4 := pkt.ObjectIdParam(Verid)
		v := &pkt.DeleteFileReqV2_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		req.Vnu = v
	}
	_, errmsg := net.RequestSN(req, self.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[DeleteObject][%d]%s/%s ERR:%s\n", self.UClient.UserId, buck, fileName, pkt.ToError(errmsg))
		return errmsg
	} else {
		logrus.Infof("[DeleteObject][%d]%s/%s OK.\n", self.UClient.UserId, buck, fileName)
		return nil
	}
}

func (self *ObjectAccessor) ObjectExist(buck, fileName string) (bool, *pkt.ErrorMessage) {
	id, err := self.GetObjectId(buck, fileName)
	if err != nil {
		return false, err
	} else {
		if id == primitive.NilObjectID {
			return false, nil
		} else {
			return true, nil
		}
	}
}

func (self *ObjectAccessor) GetObjectId(buck, fileName string) (primitive.ObjectID, *pkt.ErrorMessage) {
	id, err := self.GetObject(buck, fileName)
	if err != nil {
		return primitive.NilObjectID, err
	} else {
		return id.FileId, nil
	}
}

var Object_CACHE = cache.New(time.Duration(5)*time.Second, time.Duration(5)*time.Second)

func (self *ObjectAccessor) GetObject(buck, fileName string) (*FileItem, *pkt.ErrorMessage) {
	key := fmt.Sprintf("[%d]%s/%s", self.UClient.UserId, buck, fileName)
	v, found := Object_CACHE.Get(key)
	if found {
		return v.(*FileItem), nil
	}
	req := &pkt.GetObjectReqV2{
		UserId:     &self.UClient.UserId,
		SignData:   &self.UClient.SignKey.Sign,
		KeyNumber:  &self.UClient.SignKey.KeyNumber,
		BucketName: &buck,
		FileName:   &fileName,
	}
	resp, errmsg := net.RequestSN(req, self.UClient.SuperNode, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[GetObject]%s ERR:%s\n", key, pkt.ToError(errmsg))
		return nil, errmsg
	}
	dresp, OK := resp.(*pkt.GetObjectResp)
	if OK {
		item := &FileItem{}
		if dresp.Id == nil || dresp.Id.Timestamp == nil || dresp.Id.MachineIdentifier == nil || dresp.Id.ProcessIdentifier == nil || dresp.Id.Counter == nil {
			return item, nil
		}
		item.FileId = pkt.NewObjectId(*dresp.Id.Timestamp, *dresp.Id.MachineIdentifier, *dresp.Id.ProcessIdentifier, *dresp.Id.Counter)
		item.FileName = fileName
		logrus.Infof("[GetObject]%s OK\n", key)
		Object_CACHE.SetDefault(key, item)
		return item, nil
	} else {
		logrus.Errorf("[GetObject]%s return err msg type.\n", key)
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
}
