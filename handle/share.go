package handle

import (
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

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
		return nil, WRITE_ROUTINE_NUM, h.user.Routine
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
