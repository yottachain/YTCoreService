package handle

import (
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson"
)

type TotalHandler struct {
	pkey string
	m    *pkt.TotalReq
}

func (h *TotalHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.TotalReq)
	if ok {
		h.m = req
		return nil, HTTP_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *TotalHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[TotalUsers]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	m, err := dao.TotalUsers()
	if err != nil {
		logrus.Errorf("[TotalUsers]ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	usedspace := uint64(m.Usedspace)
	spaceTotal := uint64(m.SpaceTotal)
	fileTotal := uint64(m.FileTotal)
	blkCount, err := dao.GetBlockCount()
	if err != nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	blk_LinkCount, err := dao.GetBlockNlinkCount()
	if err != nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	blk_LinkCount = blk_LinkCount + blkCount
	res := &pkt.TotalResp{FileTotal: &fileTotal, SpaceTotal: &spaceTotal,
		Usedspace: &usedspace, BkTotal: &blkCount, ActualBkTotal: &blk_LinkCount}
	return res
}

type UserSpaceHandler struct {
	pkey string
	m    *pkt.UserSpaceReq
}

func (h *UserSpaceHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UserSpaceReq)
	if ok {
		h.m = req
		if h.m.Userid == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, HTTP_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UserSpaceHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[TotalUsers]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	user := dao.GetUserByUserId(int32(*h.m.Userid))
	if user == nil {
		return pkt.NewErrorMsg(pkt.INVALID_USER_ID, err.Error())
	}
	ress := user.GetTotalJson()
	return &pkt.UserSpaceResp{Jsonstr: &ress}
}

type UserListHandler struct {
	pkey string
	m    *pkt.UserListReq
}

func (h *UserListHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UserListReq)
	if ok {
		h.m = req
		if h.m.LastId == nil {
			h.m.LastId = new(int32)
			*h.m.LastId = -1
		}
		if h.m.Count == nil {
			h.m.Count = new(int32)
			*h.m.Count = 1000
		} else {
			*h.m.Count = int32(env.CheckInt(int(*h.m.Count), 100, 1000))
		}
		return nil, HTTP_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UserListHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[UserList]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	ls, err := dao.ListUsers(*h.m.LastId, int(*h.m.Count), bson.M{"_id": 1, "username": 1, "spaceTotal": 1})
	if err != nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	spaces := []*pkt.UserListResp_UserSpace{}
	for _, user := range ls {
		uid := uint32(user.UserID)
		resp := &pkt.UserListResp_UserSpace{UserId: &uid, UserName: &user.Username, SpaceTotal: &user.SpaceTotal}
		spaces = append(spaces, resp)
	}
	return &pkt.UserListResp{Userspace: spaces}
}

type RelationshipHandler struct {
	pkey string
	m    *pkt.Relationship
}

func (h *RelationshipHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.Relationship)
	if ok {
		h.m = req
		if h.m.Username == nil || h.m.MpoolOwner == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, HTTP_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *RelationshipHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[Relationship]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	err = dao.SetRelationship(*h.m.Username, *h.m.MpoolOwner)
	if err != nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	} else {
		return &pkt.VoidResp{}
	}
}
