package handle

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var Upload_CACHE = cache.New(3*time.Minute, 1*time.Minute)

func SetUploadObject(vnu primitive.ObjectID, ca *UploadObjectCache) {
	Upload_CACHE.SetDefault(vnu.Hex(), ca)
}

func GetUploadObject(vnu primitive.ObjectID) *UploadObjectCache {
	var ca *UploadObjectCache
	v, found := Upload_CACHE.Get(vnu.Hex())
	if found {
		ca, _ = v.(*UploadObjectCache)
	}
	return ca
}

func DelUploadObject(vnu primitive.ObjectID) {
	Upload_CACHE.Delete(vnu.Hex())
}

func LoadUploadObject(userid int32, vnu primitive.ObjectID) (*UploadObjectCache, error) {
	var ca *UploadObjectCache
	v, found := Upload_CACHE.Get(vnu.Hex())
	if found {
		ca, _ = v.(*UploadObjectCache)
	} else {
		meta := &dao.ObjectMeta{UserId: userid, VNU: vnu}
		err := meta.GetByVNU()
		if err != nil {
			return nil, err
		}
		nums := pkt.ReferIds(pkt.ParseRefers(meta.BlockList))
		ca = &UploadObjectCache{UserId: userid, BlockList: nums}
	}
	return ca, nil
}

type UploadObjectCache struct {
	sync.RWMutex
	UserId    int32
	BlockList []uint32
}

func (self *UploadObjectCache) AddBlocks(ids []uint32) {
	self.Lock()
	if self.BlockList == nil {
		self.BlockList = []uint32{}
	}
	for _, id := range ids {
		b := false
		for _, sid := range self.BlockList {
			if sid == id {
				b = true
				break
			}
		}
		if !b {
			self.BlockList = append(self.BlockList, id)
		}
	}
	self.Unlock()
}

func (self *UploadObjectCache) AddBlock(id uint32) {
	self.Lock()
	if self.BlockList == nil {
		self.BlockList = []uint32{}
	}
	b := false
	for _, sid := range self.BlockList {
		if sid == id {
			b = true
			break
		}
	}
	if !b {
		self.BlockList = append(self.BlockList, id)
	}
	self.Unlock()
}

func (self *UploadObjectCache) Exists(id uint32) bool {
	self.RLock()
	defer self.RUnlock()
	for _, bid := range self.BlockList {
		if id == bid {
			return true
		}
	}
	return false
}

type UploadObjectInitHandler struct {
	pkey string
	m    *pkt.UploadObjectInitReqV2
	user *dao.User
}

func (h *UploadObjectInitHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadObjectInitReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Length == nil || h.m.VHW == nil {
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

func (h *UploadObjectInitHandler) Handle() proto.Message {
	logrus.Infof("[UploadOBJInit]Init %d\n", h.user.UserID)
	n := net.GetUserSuperNode(h.user.UserID)
	if n.ID != int32(env.SuperNodeID) {
		logrus.Infof("[UploadOBJInit]UID:%d,INVALID_USER_ID\n", h.user.UserID)
		return pkt.NewError(pkt.INVALID_USER_ID)
	}
	if len(h.m.VHW) != 32 {
		return pkt.NewError(pkt.INVALID_VHW)
	}
	has, err := net.HasSpace(*h.m.Length, h.user.Username)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if !has {
		return pkt.NewError(pkt.NOT_ENOUGH_DHH)
	}
	meta := dao.NewObjectMeta(h.user.UserID, h.m.VHW)
	exists, err := meta.IsExists()
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	ca := &UploadObjectCache{UserId: h.user.UserID}
	resp := &pkt.UploadObjectInitResp{Repeat: new(bool)}
	*resp.Repeat = false
	if exists {
		i1, i2, i3, i4 := pkt.ObjectIdParam(meta.VNU)
		resp.Vnu = &pkt.UploadObjectInitResp_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		if meta.NLINK == 0 {
			nums := pkt.ReferIds(pkt.ParseRefers(meta.BlockList))
			ca.AddBlocks(nums)
			if meta.Length != *h.m.Length {
				meta.Length = *h.m.Length
				err = meta.UpdateLength()
				if err != nil {
					return pkt.NewError(pkt.SERVER_ERROR)
				}
			}
			logrus.Debugf("[UploadOBJInit]UID:%d,Uploading...\n", h.user.UserID)
			count := uint32(len(nums))
			resp.Blocks = &pkt.UploadObjectInitResp_Blocks{Count: &count, Blocks: nums}
		} else {
			err = meta.INCObjectNLINK()
			if err != nil {
				return pkt.NewError(pkt.SERVER_ERROR)
			}
			*resp.Repeat = true
			logrus.Debugf("[UploadOBJInit]UID:%d,Already exists.\n", h.user.UserID)
			return resp
		}
	} else {
		meta.VNU = primitive.NewObjectID()
		meta.Length = *h.m.Length
		meta.NLINK = 0
		meta.Usedspace = 0
		meta.BlockList = [][]byte{}
		err = meta.Insert()
		if err != nil {
			return pkt.NewError(pkt.SERVER_ERROR)
		}
		i1, i2, i3, i4 := pkt.ObjectIdParam(meta.VNU)
		resp.Vnu = &pkt.UploadObjectInitResp_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	}
	logrus.Infof("[UploadOBJInit]UID:%d,UploadId:%s.\n", h.user.UserID, meta.VNU.Hex())
	SetUploadObject(meta.VNU, ca)
	h.sign(resp, meta.VNU)
	return resp
}

func (h *UploadObjectInitHandler) sign(resp *pkt.UploadObjectInitResp, vnu primitive.ObjectID) {
	t := uint64(time.Now().Unix())
	resp.Stamp = &t
	bytebuf := bytes.NewBuffer([]byte{})
	bytebuf.WriteString(vnu.Hex())
	bytebuf.WriteString(h.pkey)
	bytebuf.WriteString(strconv.FormatInt(int64(t), 10))
	signdata, err := YTCrypto.Sign(net.GetLocalSuperNode().PrivKey, bytebuf.Bytes())
	if err != nil {
		logrus.Errorf("[UploadOBJInit]UploadObjectInitResp Sign ERR%s\n", err)
	} else {
		resp.SignArg = &signdata
	}
}

func SaveObjectMeta(req *pkt.SaveObjectMetaReq, refer *pkt.Refer, v primitive.ObjectID) (proto.Message, *pkt.ErrorMessage) {
	sn := net.GetUserSuperNode(*req.UserID)
	if sn.ID == int32(env.SuperNodeID) {
		handler := &SaveObjectMetaHandler{pkey: sn.PubKey, m: req, refer: refer, vnu: v}
		msg := handler.Handle()
		if err, ok := msg.(*pkt.ErrorMessage); ok {
			return nil, err
		} else {
			return msg, nil
		}
	} else {
		msg, err := net.RequestSN(req, sn, "", 0, true)
		if err != nil {
			return nil, err
		} else {
			return msg, nil
		}
	}
}

type SaveObjectMetaHandler struct {
	pkey  string
	m     *pkt.SaveObjectMetaReq
	refer *pkt.Refer
	vnu   primitive.ObjectID
}

func (h *SaveObjectMetaHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.SaveObjectMetaReq)
	if ok {
		h.m = req
		if h.m.UserID == nil || h.m.VNU == nil || h.m.UsedSpace == nil || h.m.Mode == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.refer = pkt.NewRefer(h.m.Refer)
		if h.refer == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Refer is Null value"), nil, nil
		}
		v, err := primitive.ObjectIDFromHex(*h.m.VNU)
		if err != nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Invalid VNU"), nil, nil
		}
		h.vnu = v
		return nil, SYNC_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *SaveObjectMetaHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[SaveObjectMeta]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	logrus.Infof("[SaveObjectMeta]/%d/%s/%d/%d\n", *h.m.UserID, *h.m.VNU, h.refer.Id, h.refer.VBI)
	if h.refer.VBI == 0 {
		return pkt.NewError(pkt.INVALID_UPLOAD_ID)
	}
	ca, err := LoadUploadObject(*h.m.UserID, h.vnu)
	if ca == nil {
		return pkt.NewError(pkt.INVALID_UPLOAD_ID)
	}
	resp := &pkt.SaveObjectMetaResp{Exists: new(bool)}
	if ca.Exists(uint32(h.refer.Id)) {
		*resp.Exists = true
	} else {
		*resp.Exists = false
		err = dao.AddRefer(uint32(*h.m.UserID), h.vnu, h.m.Refer, *h.m.UsedSpace)
		if err != nil {
			return pkt.NewError(pkt.SERVER_ERROR)
		}
		ca.AddBlock(uint32(h.refer.Id))
		if *h.m.Mode {
			dao.IncBlockCount()
		}
	}
	SetUploadObject(h.vnu, ca)
	return resp
}

type ActiveCacheHandler struct {
	pkey string
	m    *pkt.ActiveCacheV2
	user *dao.User
	vnu  primitive.ObjectID
}

func (h *ActiveCacheHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.ActiveCacheV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Vnu == nil {
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

func (h *ActiveCacheHandler) Handle() proto.Message {
	ca := GetUploadObject(h.vnu)
	if ca != nil {
		SetUploadObject(h.vnu, ca)
	}
	return &pkt.VoidResp{}
}

type UploadObjectEndHandler struct {
	pkey string
	m    *pkt.UploadObjectEndReqV2
	user *dao.User
	vnu  primitive.ObjectID
}

func (h *UploadObjectEndHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadObjectEndReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Vnu == nil || h.m.VHW == nil {
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

func (h *UploadObjectEndHandler) Handle() proto.Message {
	ca := GetUploadObject(h.vnu)
	if ca == nil {
		logrus.Warnf("[UploadOBJEnd][%s]Already completed.\n", h.vnu.Hex())
		return pkt.NewError(pkt.INVALID_UPLOAD_ID)
	}
	meta := dao.NewObjectMeta(h.user.UserID, h.m.VHW)
	err := meta.GetAndUpdateNlink()
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	usedspace := meta.Usedspace
	unitspace := uint64(1024 * 16)
	addusedspace := usedspace / unitspace
	if usedspace%unitspace > 1 {
		addusedspace = addusedspace + 1
	}
	err = dao.UpdateUserSpace(h.user.UserID, int64(usedspace), 1, int64(meta.Length))
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	DelUploadObject(meta.VNU)
	if usedspace <= env.PCM {
		dao.AddNewObject(meta.VNU, usedspace, h.user.UserID, h.user.Username, 0)
		logrus.Infof("[UploadOBJEnd][%d]File length less than 16K,Delay billing...\n", h.user.UserID)
		return &pkt.VoidResp{}
	}
	err = net.AddUsedSpace(h.user.Username, addusedspace)
	if err != nil {
		dao.AddNewObject(meta.VNU, usedspace, h.user.UserID, h.user.Username, 0)
		logrus.Errorf("[UploadOBJEnd][%d]Add usedSpace ERR:%s\n", h.user.UserID, err)
		return &pkt.VoidResp{}
	}
	logrus.Infof("[UploadOBJEnd][%d]Add usedSpace:%d\n", h.user.UserID, usedspace)
	firstCost := CalFirstFee(int64(usedspace))
	err = net.SubBalance(h.user.Username, firstCost)
	if err != nil {
		dao.AddNewObject(meta.VNU, usedspace, h.user.UserID, h.user.Username, 1)
		logrus.Errorf("[UploadOBJEnd][%d]Sub Balance ERR:%s\n", h.user.UserID, err)
	}
	logrus.Infof("[UploadOBJEnd][%d]Sub balance:%d\n", h.user.UserID, firstCost)
	logrus.Infof("[UploadOBJEnd]/%d/%s OK.\n", h.user.UserID, meta.VNU.Hex())
	return &pkt.VoidResp{}
}
