package handle

import (
	"bytes"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/eos"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCrypto"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/proto"
)

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
		if len(h.m.VHW) != 32 {
			return pkt.NewError(pkt.INVALID_VHW), nil, nil
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
	if len(h.m.VHW) != 32 {
		return pkt.NewError(pkt.INVALID_VHW)
	}
	flag, err := eos.CheckFreeSpace(h.user.UserID)
	if err != nil {
		logrus.Errorf("[UploadOBJInit][%d]CheckFreeSpace ERR:%s\n", h.user.UserID, err)
	}
	if !flag {
		has, err := eos.HasSpace(*h.m.Length, h.user.Username)
		if err != nil {
			return pkt.NewError(pkt.SERVER_ERROR)
		}
		if !has {
			return pkt.NewError(pkt.NOT_ENOUGH_DHH)
		}
	}
	meta := dao.NewObjectMeta(h.user.UserID, h.m.VHW)
	exists, err := meta.IsExists()
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	resp := &pkt.UploadObjectInitResp{Repeat: new(bool)}
	*resp.Repeat = false
	if exists {
		i1, i2, i3, i4 := pkt.ObjectIdParam(meta.VNU)
		resp.Vnu = &pkt.UploadObjectInitResp_VNU{Timestamp: i1, MachineIdentifier: i2, ProcessIdentifier: i3, Counter: i4}
		if meta.NLINK == 0 {
			nums := pkt.ReferIds(pkt.ParseRefers(meta.BlockList))
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
	signdata, err := YTCrypto.Sign(net.SuperNode.PrivKey, bytebuf.Bytes())
	if err != nil {
		logrus.Errorf("[UploadOBJInit]UploadObjectInitResp Sign ERR%s\n", err)
	} else {
		resp.SignArg = &signdata
	}
}

func SaveObjectMeta(user *dao.User, refer *pkt.Refer, vnu primitive.ObjectID) *pkt.ErrorMessage {
	logrus.Infof("[SaveObjectMeta]/%d/%s/%d/%d\n", user.UserID, vnu, refer.Id, refer.VBI)
	err := dao.AddRefer(uint32(user.UserID), vnu, refer.Bytes())
	if err != nil {
		logrus.Errorf("[SaveObjectMeta]Save object refer:/%s/%d ERR:%s\n", vnu.Hex(), refer.Id, err)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	return nil
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

func (h *UploadObjectEndHandler) SumUsedSpace(meta *dao.ObjectMeta) uint64 {
	refs := pkt.MapRefers(pkt.ParseRefers(meta.BlockList))
	for ii := 0; ; ii++ {
		ref := refs[int32(ii)]
		if ref == nil {
			break
		}
	}
	var usedspace uint64 = 0
	for _, ref := range refs {
		var uspace uint64 = 0
		if ref.ShdCount == 0 || ref.RealSize < env.PL2 {
			uspace = uint64(env.PCM)
		} else {
			if ref.RealSize/(env.PFL-1) > 0 {
				uspace = uint64(env.PFL) * uint64(2*ref.ShdCount)
			} else {
				uspace = uint64(env.PFL) * uint64(ref.ShdCount)
			}
		}
		if ref.Dup == 0 {
			usedspace = usedspace + uspace
		} else {
			usedspace = usedspace + uspace*uint64(env.Space_factor)/100
		}
	}
	return usedspace
}

func (h *UploadObjectEndHandler) Handle() proto.Message {
	meta := dao.NewObjectMeta(h.user.UserID, h.m.VHW)
	exists, err := meta.IsExists()
	if err != nil || !exists {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if meta.NLINK > 0 {
		logrus.Warnf("[UploadOBJEnd][%s]Already completed.\n", h.vnu.Hex())
		return pkt.NewError(pkt.INVALID_UPLOAD_ID)
	}
	usedspace := h.SumUsedSpace(meta)
	if usedspace == 0 {
		logrus.Warnf("[UploadOBJEnd][%s]Zero length file.\n", h.vnu.Hex())
		return pkt.NewError(pkt.BAD_FILE)
	}
	err = meta.GetAndUpdateEnd(usedspace)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	unitspace := uint64(1024 * 16)
	addusedspace := usedspace / unitspace
	if usedspace%unitspace > 1 {
		addusedspace = addusedspace + 1
	}
	err = dao.UpdateUserSpace(h.user.UserID, int64(usedspace), 1, int64(meta.Length))
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if usedspace <= env.PCM {
		dao.AddNewObject(meta.VNU, usedspace, h.user.UserID, h.user.Username, 0)
		logrus.Infof("[UploadOBJEnd][%d]File length less than 16K,Delay billing...\n", h.user.UserID)
		return &pkt.VoidResp{}
	}
	err = eos.AddUsedSpace(h.user.Username, addusedspace)
	if err != nil {
		dao.AddNewObject(meta.VNU, usedspace, h.user.UserID, h.user.Username, 0)
		logrus.Errorf("[UploadOBJEnd][%d]Add usedSpace ERR:%s\n", h.user.UserID, err)
		return &pkt.VoidResp{}
	}
	logrus.Infof("[UploadOBJEnd][%d]Add usedSpace:%d\n", h.user.UserID, usedspace)
	flag := false
	flag, err = eos.CheckFreeSpace(h.user.UserID)
	if err != nil {
		logrus.Errorf("[UploadOBJEnd][%d]CheckFreeSpace ERR:%s\n", h.user.UserID, err)
	}
	if !flag {
		firstCost := env.CalFirstFee(int64(usedspace))
		err = eos.SubBalance(h.user.Username, firstCost)
		if err != nil {
			dao.AddNewObject(meta.VNU, usedspace, h.user.UserID, h.user.Username, 1)
			logrus.Errorf("[UploadOBJEnd][%d]Sub Balance ERR:%s\n", h.user.UserID, err)
		}
		logrus.Infof("[UploadOBJEnd][%d]Sub balance:%d\n", h.user.UserID, firstCost)
	} else {
		logrus.Infof("[UploadOBJEnd][%d]Use free space \n", h.user.UserID)
	}
	logrus.Infof("[UploadOBJEnd]/%d/%s OK.\n", h.user.UserID, meta.VNU.Hex())
	return &pkt.VoidResp{}
}

type ActiveCacheHandler struct {
	m *pkt.ActiveCacheV2
}

func (h *ActiveCacheHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	req, ok := msg.(*pkt.ActiveCacheV2)
	if ok {
		h.m = req
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *ActiveCacheHandler) Handle() proto.Message {
	return &pkt.VoidResp{}
}
