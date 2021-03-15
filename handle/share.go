package handle

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type AuthHandler struct {
	pkey          string
	m             *pkt.AuthReq
	user          *dao.User
	authuser      *dao.User
	authkeynumber int32
	authbucketid  primitive.ObjectID
}

func (h *AuthHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.AuthReq)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Username == nil || h.m.Pubkey == nil || h.m.Bucketname == nil || *h.m.Bucketname == "" || h.m.FileName == nil || *h.m.FileName == "" {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.Reflist == nil || h.m.Length == nil || h.m.VHW == nil || h.m.Reflist.Refers == nil || len(h.m.Reflist.Refers) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, AUTH_ROUTINE_NUM, h.user.Routine
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *AuthHandler) Handle() proto.Message {
	logrus.Debugf("[AuthHandler][%d]Receive auth request:/%s/%s to %s\n", *h.m.UserId, *h.m.Bucketname, *h.m.FileName, *h.m.Username)
	resp := h.createBucket()
	if resp != nil {
		return resp
	}
	meta := dao.NewObjectMeta(h.authuser.UserID, h.m.VHW)
	err := meta.GetAndUpdateLink()
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	} else {
		if meta.VNU != primitive.NilObjectID {
			return h.writeMeta(meta.VNU)
		}
	}
	vbigroup := make(map[int32][]int64)
	refers := []*pkt.Refer{}
	for _, refbs := range h.m.Reflist.Refers {
		refer := pkt.NewRefer(refbs)
		if refer == nil {
			logrus.Errorf("[AuthHandler][%d]Refer data err\n", *h.m.UserId)
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Refer data err")
		}
		refer.KeyNumber = int16(h.authkeynumber)
		refers = append(refers, refer)
		ids := vbigroup[int32(refer.SuperID)]
		ids = append(ids, refer.VBI)
		vbigroup[int32(refer.SuperID)] = ids
	}
	startTime := time.Now()
	usedspaces := make([]int64, len(vbigroup))
	wgroup := sync.WaitGroup{}
	index := 0
	for k, v := range vbigroup {
		wgroup.Add(1)
		go h.addNLink(k, v, &usedspaces[index], &wgroup)
		index++
	}
	wgroup.Wait()
	usedspace := int64(0)
	for _, us := range usedspaces {
		if us == -1 {
			return pkt.NewError(pkt.SERVER_ERROR)
		} else {
			usedspace = usedspace + us
		}
	}
	logrus.Infof("[AuthHandler][%d]Sum fee result %d,add nlink %d,take times %d ms\n", *h.m.UserId, usedspace,
		len(refers), time.Now().Sub(startTime).Milliseconds())
	VNU := primitive.NewObjectID()
	er := h.addMeta(uint64(usedspace), VNU, refers)
	if er != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	logrus.Infof("[AuthHandler][%d]Auth object /%s/%s to %s OK\n", *h.m.UserId, *h.m.Bucketname, *h.m.FileName, *h.m.Username)
	h.doFee(uint64(usedspace), VNU)
	return h.writeMeta(VNU)
}

func (h *AuthHandler) ReqHashCode() string {
	md5Digest := md5.New()
	ss := fmt.Sprintf("%d%s%s%s%s", h.user.UserID, *h.m.Bucketname, *h.m.FileName, *h.m.Username, *h.m.Pubkey)
	md5Digest.Write([]byte(ss))
	md5Digest.Write(h.m.VHW)
	return string(md5Digest.Sum(nil))
}

const AUTH_BUCKET = "share"

var AUTH_MAP sync.Map

func (h *AuthHandler) createBucket() proto.Message {
	hash := h.ReqHashCode()
	_, ok := AUTH_MAP.Load(hash)
	if ok {
		logrus.Errorf("[AuthHandler][%d]REPEAT_REQ...\n", *h.m.UserId)
		pkt.NewErrorMsg(pkt.REPEAT_REQ, "REPEAT_REQ")
	}
	AUTH_MAP.Store(hash, "")
	defer AUTH_MAP.Delete(hash)
	h.authuser = dao.GetUserByUsername(*h.m.Username)
	if h.authuser == nil {
		logrus.Errorf("[AuthHandler][%d]Invalid Username:%s\n", *h.m.UserId, *h.m.Username)
		return pkt.NewErrorMsg(pkt.INVALID_USER_ID, "Invalid Username:"+*h.m.Username)
	}
	bs, _ := base58.Decode(*h.m.Pubkey)
	if bs == nil {
		logrus.Errorf("[AuthHandler][%d]Invalid Pubkey:%s\n", h.authuser.UserID, *h.m.Pubkey)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Pubkey err")
	}
	h.authkeynumber = -1
	for index, k := range h.authuser.KUEp {
		if bytes.Equal(k, bs) {
			h.authkeynumber = int32(index)
			break
		}
	}
	if h.authkeynumber == -1 {
		logrus.Errorf("[AuthHandler][%d]Pubkey:%s non-existent\n", h.authuser.UserID, *h.m.Pubkey)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Pubkey non-existent")
	}
	meta, _ := dao.GetBucketIdFromCache(AUTH_BUCKET, h.authuser.UserID)
	if meta == nil {
		meta = &dao.BucketMeta{UserId: h.authuser.UserID, BucketId: primitive.NewObjectID(), BucketName: AUTH_BUCKET}
		err := dao.SaveBucketMeta(meta)
		if err != nil {
			return pkt.NewError(pkt.SERVER_ERROR)
		}
		logrus.Infof("[AuthHandler][%d]Create share bucket\n", h.authuser.UserID)
		dao.DelBucketListCache(h.authuser.UserID)
	}
	h.authbucketid = meta.BucketId
	return nil
}

func (h *AuthHandler) writeMeta(vnu primitive.ObjectID) proto.Message {
	fmeta := &dao.FileMeta{UserId: h.authuser.UserID, BucketId: h.authbucketid, VersionId: vnu, Meta: h.m.Meta, Acl: []byte{}}
	fmeta.FileName = *h.m.Bucketname + "/" + *h.m.FileName
	err := fmeta.SaveFileMeta()
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	logrus.Infof("[AuthHandler][%d]Write meta:/share/%s\n", h.authuser.UserID, fmeta.FileName)
	OBJ_ADD_LIST_CACHE.SetDefault(strconv.Itoa(int(h.user.UserID)), time.Now())
	return &pkt.VoidResp{}
}

func (h *AuthHandler) addMeta(usedspace uint64, VNU primitive.ObjectID, refers []*pkt.Refer) error {
	meta := dao.NewObjectMeta(h.authuser.UserID, h.m.VHW)
	meta.BlockList = [][]byte{}
	for _, ref := range refers {
		meta.BlockList = append(meta.BlockList, ref.Bytes())
	}
	meta.Length = *h.m.Length
	meta.NLINK = 1
	meta.Usedspace = usedspace
	meta.VNU = VNU
	return meta.Insert()
}

func (h *AuthHandler) doFee(usedspace uint64, VNU primitive.ObjectID) {
	unitspace := uint64(1024 * 16)
	addusedspace := usedspace / unitspace
	if usedspace%unitspace > 1 {
		addusedspace = addusedspace + 1
	}
	dao.UpdateUserSpace(h.authuser.UserID, int64(usedspace), 1, int64(*h.m.Length))
	if usedspace <= env.PCM {
		dao.AddNewObject(VNU, usedspace, h.authuser.UserID, h.authuser.Username, 0)
		logrus.Infof("[AuthHandler][%d]File length less than 16K,Delay billing...\n", h.authuser.UserID)
	}
	err := net.AddUsedSpace(h.authuser.Username, addusedspace)
	if err != nil {
		dao.AddNewObject(VNU, usedspace, h.authuser.UserID, h.authuser.Username, 0)
		logrus.Errorf("[AuthHandler][%d]Add usedSpace ERR:%s\n", h.authuser.UserID, err)
	}
	logrus.Infof("[AuthHandler][%d]Add usedSpace:%d\n", h.authuser.UserID, usedspace)
	firstCost := CalFirstFee(int64(usedspace))
	err = net.SubBalance(h.authuser.Username, firstCost)
	if err != nil {
		dao.AddNewObject(VNU, usedspace, h.authuser.UserID, h.authuser.Username, 1)
		logrus.Errorf("[AuthHandler][%d]Sub Balance ERR:%s\n", h.authuser.UserID, err)
	}
	logrus.Infof("[AuthHandler][%d]Sub balance:%d\n", h.authuser.UserID, firstCost)
	logrus.Infof("[AuthHandler]/%d/%s OK.\n", h.authuser.UserID, VNU.Hex())
}

func (h *AuthHandler) addNLink(snid int32, vibs []int64, usedSpace *int64, wg *sync.WaitGroup) {
	defer wg.Done()
	req := &pkt.AuthBlockLinkReq{VBIS: vibs}
	var longmsg proto.Message
	sn := net.GetSuperNode(int(snid))
	if sn.ID == int32(env.SuperNodeID) {
		handler := &AuthBlockLinkHandler{pkey: sn.PubKey, m: req}
		msg := handler.Handle()
		if _, ok := msg.(*pkt.ErrorMessage); ok {
			*usedSpace = -1
			return
		} else {
			longmsg = msg
		}
	} else {
		msg, err := net.RequestSN(req, sn, "", 0, false)
		if err != nil {
			*usedSpace = -1
			return
		} else {
			longmsg = msg
		}
	}
	if resp, ok := longmsg.(*pkt.LongResp); ok {
		*usedSpace = resp.Value
	} else {
		*usedSpace = -1
	}

}

type AuthBlockLinkHandler struct {
	pkey string
	m    *pkt.AuthBlockLinkReq
}

func (h *AuthBlockLinkHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.AuthBlockLinkReq)
	if ok {
		h.m = req
		if h.m.VBIS == nil || len(h.m.VBIS) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, SUMFEE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *AuthBlockLinkHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[AuthBlockLink]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	logrus.Debugf("[AuthBlockLink]Receive request:Count %d\n", len(h.m.VBIS))
	startTime := time.Now()
	m, err := dao.GetUsedSpace(h.m.VBIS)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	err = dao.AddLinks(h.m.VBIS)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	us := h.GetUsedSpace(m)
	logrus.Debugf("[AuthBlockLink]Sum OK,count %d,take times %d ms\n", len(h.m.VBIS), time.Now().Sub(startTime).Milliseconds())
	return &pkt.LongResp{Value: us}
}

func (h *AuthBlockLinkHandler) GetUsedSpace(metas map[int64]*dao.BlockMeta) int64 {
	var space int64 = 0
	for _, id := range h.m.VBIS {
		m, ok := metas[id]
		if ok {
			if m.AR != codec.AR_DB_MODE {
				if m.NLINK > 0 {
					space = space + env.PFL*int64(m.VNF)*int64(env.Space_factor)/100
				} else {
					space = space + env.PFL*int64(m.VNF)
				}
			} else {
				space = space + int64(env.PCM)
			}
		}
	}
	return space
}

type GetFileMetaHandler struct {
	pkey  string
	m     *pkt.GetFileAuthReq
	user  *dao.User
	verid primitive.ObjectID
}

func (h *GetFileMetaHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.GetFileAuthReq)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Bucketname == nil || h.m.FileName == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.Versionid != nil {
			if h.m.Versionid.Timestamp == nil || h.m.Versionid.MachineIdentifier == nil || h.m.Versionid.ProcessIdentifier == nil || h.m.Versionid.Counter == nil {
				return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
			}
			h.verid = pkt.NewObjectId(*h.m.Versionid.Timestamp, *h.m.Versionid.MachineIdentifier, *h.m.Versionid.ProcessIdentifier, *h.m.Versionid.Counter)
		}
		return nil, READ_ROUTINE_NUM, h.user.Routine
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *GetFileMetaHandler) Handle() proto.Message {
	logrus.Infof("[DownloadFile]UID:%d,BucketName:%s,FileName:%s\n", h.user.UserID, *h.m.Bucketname, *h.m.FileName)
	bmeta, err := dao.GetBucketIdFromCache(*h.m.Bucketname, h.user.UserID)
	if err != nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	fmeta := &dao.FileMeta{UserId: h.user.UserID, BucketId: bmeta.BucketId, FileName: *h.m.FileName, VersionId: h.verid}
	err = fmeta.GetFileMeta()
	if err != nil {
		return pkt.NewError(pkt.INVALID_OBJECT_NAME)
	}
	meta := &dao.ObjectMeta{UserId: h.user.UserID, VNU: fmeta.VersionId}
	err = meta.GetByVNU()
	if err != nil {
		return pkt.NewError(pkt.INVALID_OBJECT_NAME)
	}
	size := uint32(len(meta.BlockList))
	refs := &pkt.GetFileAuthResp_RefList{Count: &size, Refers: meta.BlockList}
	return &pkt.GetFileAuthResp{Reflist: refs, Length: &meta.Length, VHW: meta.VHW, Meta: fmeta.Meta}
}

type UploadBlockAuthHandler struct {
	pkey string
	m    *pkt.UploadBlockAuthReq
	user *dao.User
	vnu  primitive.ObjectID
	ref  *pkt.Refer
}

func (h *UploadBlockAuthHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockAuthReq)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		h.ref = pkt.NewRefer(req.Refer)
		if h.ref == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:REF Null value"), nil, nil
		}
		if h.m.Vnu != nil {
			if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
				return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
			}
			h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		}
		return nil, AUTH_ROUTINE_NUM, h.user.Routine
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UploadBlockAuthHandler) Handle() proto.Message {
	logrus.Infof("[UploadBLKAuth]/%d/%s/%d...\n", h.user.UserID, h.vnu.Hex(), h.ref.Id)
	meta, _ := dao.GetBlockById(h.ref.VBI)
	if meta == nil {
		return pkt.NewError(pkt.NO_SUCH_BLOCK)
	}
	usedSpace := env.PCM
	if meta.AR != codec.AR_DB_MODE {
		usedSpace = env.PFL * uint64(meta.VNF+1) * uint64(env.Space_factor) / 100
	}
	vnustr := h.vnu.Hex()
	saveObjectMetaReq := &pkt.SaveObjectMetaReq{UserID: &h.user.UserID, VNU: &vnustr,
		Refer: h.m.Refer, UsedSpace: &usedSpace, Mode: new(bool)}
	*saveObjectMetaReq.Mode = false
	res, perr := SaveObjectMeta(saveObjectMetaReq, h.ref, h.vnu)
	if perr != nil {
		return perr
	} else {
		if saveObjectMetaResp, ok := res.(*pkt.SaveObjectMetaResp); ok {
			if saveObjectMetaResp.Exists != nil && *saveObjectMetaResp.Exists == true {
				logrus.Warnf("[UploadBLKAuth]Block %d/%s/%d has been uploaded.\n", h.user.UserID, h.vnu.Hex(), h.ref.Id)
			} else {
				dao.INCBlockNLINK(meta)
			}
		}
	}
	return &pkt.VoidResp{}
}
