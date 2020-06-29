package handle

import (
	"bytes"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/patrickmn/go-cache"
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
		if meta.UserId == userid {
			nums := pkt.ReferIds(pkt.ParseRefers(meta.BlockList))
			ca = &UploadObjectCache{UserId: userid, BlockList: nums}
		}
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

func (h *UploadObjectInitHandler) CheckRoutine() *int32 {
	if atomic.LoadInt32(WRITE_ROUTINE_NUM) > env.MAX_WRITE_ROUTINE {
		return nil
	}
	atomic.AddInt32(WRITE_ROUTINE_NUM, 1)
	return WRITE_ROUTINE_NUM
}

func (h *UploadObjectInitHandler) SetMessage(pubkey string, msg proto.Message) *pkt.ErrorMessage {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadObjectInitReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Length == nil || h.m.VHW == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value")
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE)
		}
		return nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request")
	}
}

func (h *UploadObjectInitHandler) Handle() proto.Message {
	env.Log.Infof("Upload object init %d\n", h.user.UserID)
	n := net.GetUserSuperNode(h.user.UserID)
	if n.ID != int32(env.SuperNodeID) {
		env.Log.Infof("Upload object init %d,INVALID_USER_ID\n", h.user.UserID)
		return pkt.NewError(pkt.INVALID_USER_ID)
	}
	if len(h.m.VHW) != 32 {
		return pkt.NewError(pkt.INVALID_VHW)
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
				env.Log.Warnf("Upload object %d,File length inconsistency.\n", h.user.UserID)
			} else {
				env.Log.Debugf("Upload object %d,Uploading...\n", h.user.UserID)
			}
			resp.Blocks = nums
		} else {
			err = meta.INCObjectNLINK()
			if err != nil {
				return pkt.NewError(pkt.SERVER_ERROR)
			}
			*resp.Repeat = true
			env.Log.Debugf("Upload object %d,Already exists.\n", h.user.UserID)
			return resp
		}
	} else {
		meta.VNU = primitive.NewObjectID()
		i1, i2, i3, i4 := pkt.ObjectIdParam(meta.VNU)
		resp.Vnu = &pkt.UploadObjectInitResp_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
	}
	has, err := net.HasSpace(*h.m.Length, h.user.Username)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if has {
		meta.Length = *h.m.Length
		meta.NLINK = 0
		meta.BlockList = [][]byte{}
		err = meta.InsertOrUpdate()
		if err != nil {
			return pkt.NewError(pkt.SERVER_ERROR)
		}
	} else {
		return pkt.NewError(pkt.NOT_ENOUGH_DHH)
	}
	env.Log.Infof("Upload object %d,UploadId:%s.\n", h.user.UserID, meta.VNU.Hex())
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
		env.Log.Errorf("UploadObjectInitResp Sign ERR%s\n", err)
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
		msg, err := net.RequestSN(req, sn, "", 0)
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

func (h *SaveObjectMetaHandler) CheckRoutine() *int32 {
	if atomic.LoadInt32(WRITE_ROUTINE_NUM) > env.MAX_WRITE_ROUTINE {
		return nil
	}
	atomic.AddInt32(WRITE_ROUTINE_NUM, 1)
	return WRITE_ROUTINE_NUM
}

func (h *SaveObjectMetaHandler) SetMessage(pubkey string, msg proto.Message) *pkt.ErrorMessage {
	h.pkey = pubkey
	req, ok := msg.(*pkt.SaveObjectMetaReq)
	if ok {
		h.m = req
		if h.m.UserID == nil || h.m.VNU == nil || h.m.UsedSpace == nil || h.m.Mode == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value")
		}
		h.refer = pkt.NewRefer(h.m.Refer)
		if h.refer == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Refer is Null value")
		}
		v, err := primitive.ObjectIDFromHex(*h.m.VNU)
		if err != nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Invalid VNU")
		}
		h.vnu = v
		return nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request")
	}
}

func (h *SaveObjectMetaHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		env.Log.Errorf("%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	env.Log.Infof("Save object meta:%d/%s/%d/%d\n", *h.m.UserID, *h.m.VNU, h.refer.Id, h.refer.VBI)
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

func (h *ActiveCacheHandler) CheckRoutine() *int32 {
	if atomic.LoadInt32(WRITE_ROUTINE_NUM) > env.MAX_WRITE_ROUTINE {
		return nil
	}
	atomic.AddInt32(WRITE_ROUTINE_NUM, 1)
	return WRITE_ROUTINE_NUM
}

func (h *ActiveCacheHandler) SetMessage(pubkey string, msg proto.Message) *pkt.ErrorMessage {
	h.pkey = pubkey
	req, ok := msg.(*pkt.ActiveCacheV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Vnu == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value")
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE)
		}
		if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value")
		}
		h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		return nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request")
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

func (h *UploadObjectEndHandler) CheckRoutine() *int32 {
	if atomic.LoadInt32(WRITE_ROUTINE_NUM) > env.MAX_WRITE_ROUTINE {
		return nil
	}
	atomic.AddInt32(WRITE_ROUTINE_NUM, 1)
	return WRITE_ROUTINE_NUM
}

func (h *UploadObjectEndHandler) SetMessage(pubkey string, msg proto.Message) *pkt.ErrorMessage {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadObjectEndReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Vnu == nil || h.m.VHW == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value")
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE)
		}
		if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value")
		}
		h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		return nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request")
	}
}

func (h *UploadObjectEndHandler) Handle() proto.Message {
	ca := GetUploadObject(h.vnu)
	if ca == nil {
		env.Log.Warnf("[%s] already completed.\n", h.vnu.Hex())
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
	err = dao.UpdateUserSpace(h.user.UserID, usedspace, 1, meta.Length)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	err = net.AddUsedSpace(h.user.Username, addusedspace)
	if err != nil {
		dao.AddNewObject(meta.VNU, usedspace, h.user.UserID, h.user.Username, 0)
		env.Log.Errorf("[%d] Add usedSpace ERR:%s\n", h.user.UserID, err)
		env.Log.Infof("Upload object %d/%s OK.\n", h.user.UserID, meta.VNU.Hex())
		DelUploadObject(meta.VNU)
		return &pkt.VoidResp{}
	}
	env.Log.Infof("User [%d] add usedSpace:%d\n", h.user.UserID, usedspace)
	firstCost := env.UnitFirstCost * usedspace / env.UnitSpace
	err = net.SubBalance(h.user.Username, firstCost)
	if err != nil {
		dao.AddNewObject(meta.VNU, usedspace, h.user.UserID, h.user.Username, 1)
		env.Log.Errorf("[%d] Sub Balance ERR:%s\n", h.user.UserID, err)
	}
	env.Log.Infof("User [%d] sub balance:%d\n", h.user.UserID, firstCost)
	env.Log.Infof("Upload object %d/%s OK.\n", h.user.UserID, meta.VNU.Hex())
	DelUploadObject(meta.VNU)
	return &pkt.VoidResp{}
}

func DoCacheActionLoop() {
	time.Sleep(time.Duration(30) * time.Second)
	for {
		if !DoCacheAction() {
			time.Sleep(time.Duration(30) * time.Second)
		}
	}
}

func DoCacheAction() bool {
	action := dao.FindOneNewObject()
	if action == nil {
		return false
	}
	usedspace := action.UsedSpace
	if action.Step == 0 {
		unitspace := uint64(1024 * 16)
		addusedspace := usedspace / unitspace
		if usedspace%unitspace > 1 {
			addusedspace = addusedspace + 1
		}
		err := net.AddUsedSpace(action.Username, addusedspace)
		if err != nil {
			dao.AddAction(action)
			env.Log.Errorf("[%d] Add usedSpace ERR:%s\n", action.UserID, err)
			time.Sleep(time.Duration(3) * time.Minute)
			return true
		}
		env.Log.Infof("User [%d] add usedSpace:%d\n", action.UserID, addusedspace)
	}
	firstCost := env.UnitFirstCost * usedspace / env.UnitSpace
	err := net.SubBalance(action.Username, firstCost)
	if err != nil {
		action.Step = 1
		dao.AddAction(action)
		env.Log.Errorf("[%d] Sub Balance ERR:%s\n", action.UserID, err)
		time.Sleep(time.Duration(3) * time.Minute)
	} else {
		env.Log.Infof("User [%d] sub balance:%d\n", action.UserID, firstCost)
	}
	return true
}
