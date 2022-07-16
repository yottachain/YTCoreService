package handle

import (
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/protobuf/proto"
)

type CreateBucketHandler struct {
	pkey string
	m    *pkt.CreateBucketReqV2
	user *dao.User
}

func (h *CreateBucketHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.CreateBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil || h.m.Meta == nil {
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

func (h *CreateBucketHandler) Handle() proto.Message {
	logrus.Infof("[CreateBucket]UID:%d,Name:%s\n", h.user.UserID, *h.m.BucketName)
	name := *h.m.BucketName
	if len(name) < 1 || len(name) > 20 {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	num, err := dao.GetBucketCount(uint32(h.user.UserID))
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if num >= dao.Max_Bucket_count {
		logrus.Errorf("[CreateBucket]UID:%d,ERR:TOO_MANY_BUCKETS %d\n", h.user.UserID, num)
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

func (h *GetBucketHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.GetBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil {
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

func (h *GetBucketHandler) Handle() proto.Message {
	logrus.Infof("[GetBucket]:UID:%d,Name:%s\n", h.user.UserID, *h.m.BucketName)
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

func (h *DeleteBucketHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.DeleteBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil {
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

func (h *DeleteBucketHandler) Handle() proto.Message {
	logrus.Infof("[Deletebucket]UID:%d,Name:%s\n", h.user.UserID, *h.m.BucketName)
	bmeta, err := dao.GetBucketIdFromCache(*h.m.BucketName, h.user.UserID)
	if err != nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	has, err := dao.BucketIsEmpty(uint32(h.user.UserID), bmeta.BucketId)
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
	return &pkt.VoidResp{}
}

type UpdateBucketHandler struct {
	pkey string
	m    *pkt.UpdateBucketReqV2
	user *dao.User
}

func (h *UpdateBucketHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UpdateBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.BucketName == nil || h.m.Meta == nil {
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

func (h *UpdateBucketHandler) Handle() proto.Message {
	logrus.Infof("[Updatebucket]UID:%d,Name:%s\n", h.user.UserID, *h.m.BucketName)
	bmeta, err := dao.GetBucketIdFromCache(*h.m.BucketName, h.user.UserID)
	if err != nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	nmeta := &dao.BucketMeta{BucketId: bmeta.BucketId, BucketName: bmeta.BucketName, Meta: h.m.Meta, UserId: h.user.UserID}
	err = dao.UpdateBucketMeta(nmeta)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	return &pkt.VoidResp{}
}

type ListBucketHandler struct {
	pkey string
	m    *pkt.ListBucketReqV2
	user *dao.User
}

func (h *ListBucketHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.ListBucketReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
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

func (h *ListBucketHandler) Handle() proto.Message {
	ss, err := dao.ListBucketFromCache(h.user.UserID)
	if err != nil {
		logrus.Infof("[Listbucket]UID:%d,ERR:%s\n", h.user.UserID, err)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	count := uint32(len(ss))
	bs := &pkt.ListBucketResp_Buckets{Count: &count, Names: ss}
	return &pkt.ListBucketResp{Buckets: bs}
}
