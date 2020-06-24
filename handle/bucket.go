package handle

import (
	"github.com/golang/protobuf/proto"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type CreateBucketHandler struct {
	pkey string
	m    *pkt.CreateBucketReqV2
	user *dao.User
}

func (h *CreateBucketHandler) SetPubkey(pubkey string) {
	h.pkey = pubkey
}

func (h *CreateBucketHandler) SetMessage(msg proto.Message) *pkt.ErrorMessage {
	req, ok := msg.(*pkt.CreateBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil || h.m.Meta == nil {
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

func (h *CreateBucketHandler) Handle() proto.Message {
	env.Log.Infof("Create bucket:%d/%s\n", h.user.UserID, *h.m.BucketName)
	name := *h.m.BucketName
	if len(name) < 1 || len(name) > 20 {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	num, err := dao.GetBucketCount(uint32(h.user.UserID))
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if num >= dao.Max_Bucket_count {
		env.Log.Errorf("[%d] Create bucket ERR:TOO_MANY_BUCKETS %d\n", h.user.UserID, num)
		return pkt.NewError(pkt.TOO_MANY_BUCKETS)
	}
	meta := &dao.BucketMeta{UserId: h.user.UserID, BucketId: primitive.NewObjectID(), Meta: h.m.Meta, BucketName: name}
	err = dao.SaveBucketMeta(meta)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	dao.DelBucketListCache(h.user.UserID)
	return &pkt.VoidResp{}
}

type GetBucketHandler struct {
	pkey string
	m    *pkt.GetBucketReqV2
	user *dao.User
}

func (h *GetBucketHandler) SetPubkey(pubkey string) {
	h.pkey = pubkey
}

func (h *GetBucketHandler) SetMessage(msg proto.Message) *pkt.ErrorMessage {
	req, ok := msg.(*pkt.GetBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil {
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

func (h *GetBucketHandler) Handle() proto.Message {
	env.Log.Infof("GET bucket:%d/%s\n", h.user.UserID, *h.m.BucketName)
	bmeta, err := dao.GetBucketIdFromCache(*h.m.BucketName, h.user.UserID)
	if err != nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	return &pkt.GetBucketResp{BucketName: &bmeta.BucketName, Meta: bmeta.Meta}
}

type DeleteBucketHandler struct {
	pkey string
	m    *pkt.DeleteBucketReqV2
	user *dao.User
}

func (h *DeleteBucketHandler) SetPubkey(pubkey string) {
	h.pkey = pubkey
}

func (h *DeleteBucketHandler) SetMessage(msg proto.Message) *pkt.ErrorMessage {
	req, ok := msg.(*pkt.DeleteBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil {
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

func (h *DeleteBucketHandler) Handle() proto.Message {
	env.Log.Infof("Delete bucket:%d/%s\n", h.user.UserID, *h.m.BucketName)
	bmeta, err := dao.GetBucketIdFromCache(*h.m.BucketName, h.user.UserID)
	if err != nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	has, err := dao.BucketIsEmpty(uint32(h.user.UserID))
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if !has {
		return pkt.NewError(pkt.BUCKET_NOT_EMPTY)
	}
	err = dao.DeleteBucketMeta(bmeta)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	dao.DelBucketListCache(h.user.UserID)
	dao.DelBucketCache(*h.m.BucketName, h.user.UserID)
	return &pkt.VoidResp{}
}

type UpdateBucketHandler struct {
	pkey string
	m    *pkt.UpdateBucketReqV2
	user *dao.User
}

func (h *UpdateBucketHandler) SetPubkey(pubkey string) {
	h.pkey = pubkey
}

func (h *UpdateBucketHandler) SetMessage(msg proto.Message) *pkt.ErrorMessage {
	req, ok := msg.(*pkt.UpdateBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil || h.m.Meta == nil {
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

func (h *UpdateBucketHandler) Handle() proto.Message {
	env.Log.Infof("Update bucket:%d/%s\n", h.user.UserID, *h.m.BucketName)
	bmeta, err := dao.GetBucketIdFromCache(*h.m.BucketName, h.user.UserID)
	if err != nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	nmeta := &dao.BucketMeta{BucketId: bmeta.BucketId, BucketName: bmeta.BucketName, Meta: h.m.Meta, UserId: h.user.UserID}
	err = dao.UpdateBucketMeta(nmeta)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	dao.DelBucketCache(*h.m.BucketName, h.user.UserID)
	return &pkt.VoidResp{}
}

type ListBucketHandler struct {
	pkey string
	m    *pkt.ListBucketReqV2
	user *dao.User
}

func (h *ListBucketHandler) SetPubkey(pubkey string) {
	h.pkey = pubkey
}

func (h *ListBucketHandler) SetMessage(msg proto.Message) *pkt.ErrorMessage {
	req, ok := msg.(*pkt.ListBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
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

func (h *ListBucketHandler) Handle() proto.Message {
	env.Log.Infof("List bucket:%d\n", h.user.UserID)
	ss, err := dao.ListBucketFromCache(h.user.UserID)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	count := uint32(len(ss))
	bs := &pkt.ListBucketResp_Buckets{Count: &count, Names: ss}
	return &pkt.ListBucketResp{Buckets: bs}
}
