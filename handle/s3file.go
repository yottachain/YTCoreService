package handle

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UploadFileHandler struct {
	pkey string
	m    *pkt.UploadFileReqV2
	user *dao.User
	vnu  primitive.ObjectID
}

func (h *UploadFileHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadFileReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Vnu == nil || h.m.Bucketname == nil || h.m.FileName == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UploadFileHandler) Handle() proto.Message {
	logrus.Infof("[CreateOBJ]UID:%d,%s/%s...\n", h.user.UserID, *h.m.Bucketname, *h.m.FileName)
	meta, _ := dao.GetBucketIdFromCache(*h.m.Bucketname, h.user.UserID)
	if meta == nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	m := []byte{}
	if h.m.Meta != nil {
		m = h.m.Meta
	}
	if !env.IsZeroLenFileID(h.vnu) {
		ometa := &dao.ObjectMeta{UserId: h.user.UserID, VNU: h.vnu}
		b, err := ometa.ChecekVNUExists()
		if err != nil {
			return pkt.NewError(pkt.SERVER_ERROR)
		}
		if !b {
			logrus.Errorf("[CreateOBJ]UID:%d,%s/%s ERR:INVALID_UPLOAD_ID\n", h.user.UserID, *h.m.Bucketname, *h.m.FileName)
			return pkt.NewError(pkt.INVALID_UPLOAD_ID)
		}
	}
	fmeta := &dao.FileMeta{UserId: h.user.UserID, BucketId: meta.BucketId, FileName: *h.m.FileName, VersionId: h.vnu, Meta: m, Acl: []byte{}}
	err := fmeta.SaveFileMeta()
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	OBJ_ADD_LIST_CACHE.SetDefault(strconv.Itoa(int(h.user.UserID)), time.Now())
	return &pkt.VoidResp{}
}

type CopyObjectHandler struct {
	pkey string
	m    *pkt.CopyObjectReqV2
	user *dao.User
}

func (h *CopyObjectHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.CopyObjectReqV2)
	if ok {
		h.m = req
		if h.m.SrcBucket == nil || h.m.SrcObjectKey == nil || h.m.DestBucket == nil || h.m.DestObjectKey == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Meta == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *CopyObjectHandler) Handle() proto.Message {
	logrus.Infof("[CopyObject]UID:%d,BucketName:%s,Key:%s\n", h.user.UserID, *h.m.SrcBucket, *h.m.SrcObjectKey)
	srcmeta, _ := dao.GetBucketIdFromCache(*h.m.SrcBucket, h.user.UserID)
	dstmeta, _ := dao.GetBucketIdFromCache(*h.m.DestBucket, h.user.UserID)
	if srcmeta == nil || dstmeta == nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	fmeta := &dao.FileMeta{UserId: h.user.UserID, BucketId: srcmeta.BucketId, FileName: *h.m.SrcObjectKey}
	err := fmeta.GetLastFileMeta(false)
	if err != nil {
		return pkt.NewError(pkt.INVALID_OBJECT_NAME)
	}
	meta := fmeta.Meta
	nmeta := &dao.FileMeta{UserId: h.user.UserID, BucketId: dstmeta.BucketId, FileName: *h.m.DestObjectKey,
		Meta: meta, VersionId: fmeta.VersionId, Acl: []byte{}}
	err = nmeta.SaveFileMeta()
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	OBJ_ADD_LIST_CACHE.SetDefault(strconv.Itoa(int(h.user.UserID)), time.Now())
	i1, i2, i3, i4 := pkt.ObjectIdParam(dstmeta.BucketId)
	bucketid := &pkt.CopyObjectResp_BucketId{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	ii1, ii2, ii3, ii4 := pkt.ObjectIdParam(fmeta.VersionId)
	versionid := &pkt.CopyObjectResp_VersionId{Timestamp: ii1, MachineIdentifier: ii2, ProcessIdentifier: ii3, Counter: ii4}
	return &pkt.CopyObjectResp{Bucketid: bucketid, BucketName: h.m.DestBucket, Versionid: versionid, Meta: meta}
}

type DeleteFileHandler struct {
	pkey  string
	m     *pkt.DeleteFileReqV2
	user  *dao.User
	verid primitive.ObjectID
}

func (h *DeleteFileHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.DeleteFileReqV2)
	if ok {
		h.m = req
		if h.m.Vnu != nil {
			if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
				return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
			}
			h.verid = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		}
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil || h.m.FileName == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *DeleteFileHandler) Handle() proto.Message {
	logrus.Infof("[DeleteOBJ]UID:%d,BucketName:%s,FileName:%s\n", h.user.UserID, *h.m.BucketName, *h.m.FileName)
	meta, _ := dao.GetBucketIdFromCache(*h.m.BucketName, h.user.UserID)
	if meta == nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	var err error
	fmeta := &dao.FileMeta{UserId: h.user.UserID, BucketId: meta.BucketId, FileName: *h.m.FileName, VersionId: h.verid}
	var metaWVer *dao.FileMetaWithVersion
	if h.verid == primitive.NilObjectID {
		metaWVer, err = fmeta.DeleteFileMeta()
	} else {
		metaWVer, err = fmeta.DeleteFileMetaByVersion()
	}
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if metaWVer == nil {
		return &pkt.VoidResp{}
	}
	h.decObjectNLink(metaWVer)
	OBJ_DEL_LIST_CACHE.SetDefault(strconv.Itoa(int(h.user.UserID)), time.Now())
	return &pkt.VoidResp{}
}

func (h *DeleteFileHandler) decObjectNLink(metaWVer *dao.FileMetaWithVersion) {
	for _, ver := range metaWVer.Version {
		dao.AddNeedDel(h.user.UserID, ver.VersionId)
	}
}

type GetObjectHandler struct {
	pkey string
	m    *pkt.GetObjectReqV2
	user *dao.User
}

func (h *GetObjectHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.GetObjectReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil || h.m.FileName == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		return nil, READ_ROUTINE_NUM, h.user.Routine
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *GetObjectHandler) Handle() proto.Message {
	logrus.Infof("[GetObject]UID:%d,BucketName:%s,FileName:%s\n", h.user.UserID, *h.m.BucketName, *h.m.FileName)
	meta, _ := dao.GetBucketIdFromCache(*h.m.BucketName, h.user.UserID)
	if meta == nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	fmeta := &dao.FileMeta{UserId: h.user.UserID, BucketId: meta.BucketId, FileName: *h.m.FileName}
	err := fmeta.GetLastFileMeta(true)
	if err != nil {
		return &pkt.GetObjectResp{}
	}
	i1, i2, i3, i4 := pkt.ObjectIdParam(fmeta.FileId)
	fid := &pkt.GetObjectResp_Id{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	res := &pkt.GetObjectResp{FileName: h.m.FileName, Id: fid}
	return res
}

type ListObjectHandler struct {
	pkey         string
	m            *pkt.ListObjectReqV2
	user         *dao.User
	nextid       primitive.ObjectID
	nextfileName string
	prefix       string
	limit        uint32
	version      bool
	compress     bool
	HashKey      string
}

func (h *ListObjectHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.ListObjectReqV2)
	if ok {
		h.m = req
		if h.m.Nextversionid != nil {
			if h.m.Nextversionid.Timestamp == nil || h.m.Nextversionid.MachineIdentifier == nil || h.m.Nextversionid.ProcessIdentifier == nil || h.m.Nextversionid.Counter == nil {
				return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
			}
			h.nextid = pkt.NewObjectId(*h.m.Nextversionid.Timestamp, *h.m.Nextversionid.MachineIdentifier, *h.m.Nextversionid.ProcessIdentifier, *h.m.Nextversionid.Counter)
			h.version = true
		} else {
			h.version = false
		}
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.FileName != nil {
			h.nextfileName = strings.TrimSpace(*h.m.FileName)
		}
		if h.m.Prefix != nil {
			h.prefix = *h.m.Prefix
		}
		if h.m.Limit != nil {
			h.limit = *h.m.Limit
		}
		if h.limit < 10 {
			h.limit = 10
		}
		if h.limit > 1000 {
			h.limit = 1000
		}
		if h.m.Compress != nil {
			h.compress = *h.m.Compress
		}
		h.HashKey = h.ReqHashCode(h.nextfileName, h.nextid)
		return nil, READ_ROUTINE_NUM, h.user.Routine
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *ListObjectHandler) ReqHashCode(nfile string, nversion primitive.ObjectID) string {
	md5Digest := md5.New()
	cp := 0
	if h.compress {
		cp = 1
	}
	ss := fmt.Sprintf("%d%s%d%s%s%d", h.user.UserID, *h.m.BucketName, h.limit, nfile, h.prefix, cp)
	md5Digest.Write([]byte(ss))
	if h.version {
		md5Digest.Write([]byte(nversion.Hex()))
	}
	return string(md5Digest.Sum(nil))
}

var OBJ_LIST_CACHE *cache.Cache
var OBJ_DEL_LIST_CACHE *cache.Cache
var OBJ_ADD_LIST_CACHE *cache.Cache

func InitCache() {
	OBJ_LIST_CACHE = cache.New(time.Duration(env.LsCacheExpireTime)*time.Second, time.Duration(5)*time.Second)
	OBJ_DEL_LIST_CACHE = cache.New(time.Duration(env.LsCacheExpireTime)*time.Second, time.Duration(5)*time.Second)
	OBJ_ADD_LIST_CACHE = cache.New(time.Duration(env.LsCacheExpireTime)*time.Second, time.Duration(5)*time.Second)
}

type Handles struct {
	cursorCount *int32
	lastTime    *int64
}

var ListHandles = struct {
	sync.Mutex
	handles map[int32]*Handles
}{handles: make(map[int32]*Handles)}

func (h *ListObjectHandler) Handle() proto.Message {
	cacheresp := h.checkCache()
	if cacheresp != nil {
		return cacheresp
	}
	meta, _ := dao.GetBucketIdFromCache(*h.m.BucketName, h.user.UserID)
	if meta == nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	ListHandles.Lock()
	handles, ok := ListHandles.handles[h.user.UserID]
	if !ok {
		ccount := int32(0)
		lTime := int64(0)
		handles = &Handles{cursorCount: &ccount, lastTime: &lTime}
		ListHandles.handles[h.user.UserID] = handles
	}
	ListHandles.Unlock()
	if env.LsCursorLimit <= 0 {
		ltime := atomic.LoadInt64(handles.lastTime)
		if time.Now().Unix()-ltime < int64(env.LsCacheExpireTime) {
			return pkt.NewError(pkt.TOO_MANY_CURSOR)
		} else {
			atomic.StoreInt64(handles.lastTime, time.Now().Unix())
		}
	} else {
		ccount := atomic.LoadInt32(handles.cursorCount)
		if ccount >= int32(env.LsCursorLimit) {
			return pkt.NewError(pkt.TOO_MANY_CURSOR)
		}
	}
	atomic.AddInt32(handles.cursorCount, 1)
	defer atomic.AddInt32(handles.cursorCount, -1)
	startTime := time.Now()
	resp, err := dao.ListFileMeta(uint32(h.user.UserID), meta.BucketId, h.prefix, h.nextfileName, h.nextid, int64(h.limit), h.version)
	if err != nil {
		return pkt.NewError(pkt.TOO_MANY_CURSOR)
	}
	res, count := h.doResponse(resp)
	logrus.Infof("[ListObject]UID:%d,Bucket:%s,Prefix:%s,return lines:%d,take times %d ms\n",
		h.user.UserID, *h.m.BucketName, h.prefix, count, time.Now().Sub(startTime).Milliseconds())
	if res == nil {
		res = &pkt.ListObjectResp{}
	}
	return res
}

func (h *ListObjectHandler) checkCache() proto.Message {
	v, etime, found := OBJ_LIST_CACHE.GetWithExpiration(h.HashKey)
	if !found {
		return nil
	}
	deltime, has := OBJ_DEL_LIST_CACHE.Get(strconv.Itoa(int(h.user.UserID)))
	if has {
		dtime := deltime.(time.Time)
		if etime.Sub(dtime) < time.Duration(env.LsCacheExpireTime)*time.Second {
			found = false
		}
	}
	if found {
		lastline := false
		res, ok := v.(pkt.ListObjectResp)
		if ok {
			lastline = res.LastLine
		} else {
			ress, ok1 := v.(pkt.ListObjectRespV2)
			if ok1 {
				lastline = ress.LastLine
			}
		}
		if lastline {
			addtime, has1 := OBJ_ADD_LIST_CACHE.Get(strconv.Itoa(int(h.user.UserID)))
			if has1 {
				atime := addtime.(time.Time)
				if etime.Sub(atime) < time.Duration(env.LsCacheExpireTime)*time.Second {
					found = false
				}
			}
		}
	}
	if found {
		size := OBJ_LIST_CACHE.ItemCount()
		logrus.Infof("[ListObject]UID:%d,Bucket:%s,from cache,current size:%d\n", h.user.UserID, *h.m.BucketName, size)
		return v.(proto.Message)
	}
	return nil
}

func (h *ListObjectHandler) doResponse(resp []*dao.FileMetaWithVersion) (proto.Message, int64) {
	var count int64 = 0
	linecount := 0
	var firstResp proto.Message
	res := []*pkt.ListObjectResp_FileMetaList{}
	lastkey := h.HashKey
	for _, fmeta := range resp {
		i1, i2, i3, i4 := pkt.ObjectIdParam(fmeta.FileId)
		fid := &pkt.ListObjectResp_FileMetaList_FileId{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		ii1, ii2, ii3, ii4 := pkt.ObjectIdParam(fmeta.FileId)
		bid := &pkt.ListObjectResp_FileMetaList_BucketId{Timestamp: ii1, MachineIdentifier: ii2, ProcessIdentifier: ii3, Counter: ii4}
		size := len(fmeta.Version)
		for ii, fm := range fmeta.Version {
			i1, i2, i3, i4 := pkt.ObjectIdParam(fm.VersionId)
			ver := &pkt.ListObjectResp_FileMetaList_VersionId{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
			lastv := false
			if ii >= size {
				lastv = true
			}
			m := &pkt.ListObjectResp_FileMetaList{Fileid: fid, Bucketid: bid, Versionid: ver,
				FileName: &fmeta.FileName, Meta: fm.Meta, Acl: fm.Acl, Latest: &lastv}
			res = append(res, m)
			linecount++
			count++
			if linecount >= int(h.limit) {
				linecount = 0
				if firstResp == nil {
					firstResp = h.addCache(res, lastkey, false)
				} else {
					h.addCache(res, lastkey, false)
				}
				lastkey = h.ReqHashCode(fmeta.FileName, fm.VersionId)
				res = []*pkt.ListObjectResp_FileMetaList{}
			}
		}
	}
	if len(res) > 0 {
		if firstResp == nil {
			firstResp = h.addCache(res, lastkey, true)
		} else {
			h.addCache(res, lastkey, true)
		}
	}
	return firstResp, count
}

func (h *ListObjectHandler) addCache(resp []*pkt.ListObjectResp_FileMetaList, key string, lastline bool) proto.Message {
	res := &pkt.ListObjectResp{Filemetalist: resp}
	res.LastLine = lastline
	if h.compress {
		bs, err := proto.Marshal(res)
		if err != nil {
			OBJ_LIST_CACHE.SetDefault(key, res)
			return res
		}
		buf := bytes.NewBuffer(nil)
		gw := gzip.NewWriter(buf)
		_, err = gw.Write(bs)
		gw.Close()
		ress := &pkt.ListObjectRespV2{Data: buf.Bytes()}
		ress.LastLine = lastline
		OBJ_LIST_CACHE.SetDefault(key, ress)
		return ress
	} else {
		OBJ_LIST_CACHE.SetDefault(key, res)
		return res
	}
}
