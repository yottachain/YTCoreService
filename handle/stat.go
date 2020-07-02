package handle

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

type TotalHandler struct {
	pkey string
	m    *pkt.TotalReq
}

func (h *TotalHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.TotalReq)
	if ok {
		h.m = req
		return nil, READ_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *TotalHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		env.Log.Errorf("%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	m, err := dao.TotalUsers()
	if err != nil {
		env.Log.Errorf("TotalUsers ERR%s\n", err)
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

func (h *UserSpaceHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UserSpaceReq)
	if ok {
		h.m = req
		if h.m.Userid == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil
		}
		return nil, READ_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *UserSpaceHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		env.Log.Errorf("%s\n", err)
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

func (h *UserListHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
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
		return nil, READ_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *UserListHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		env.Log.Errorf("%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	ls, err := dao.ListUsers(int(*h.m.LastId), int(*h.m.Count))
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

func (h *RelationshipHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.Relationship)
	if ok {
		h.m = req
		if h.m.Username == nil || h.m.MpoolOwner == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil
		}
		return nil, READ_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *RelationshipHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		env.Log.Errorf("%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	err = dao.SetRelationship(*h.m.Username, *h.m.MpoolOwner)
	if err != nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	} else {
		return &pkt.VoidResp{}
	}
}

type RelationshipSumHandler struct {
	pkey string
	m    *pkt.RelationShipSum
}

func (h *RelationshipSumHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.RelationShipSum)
	if ok {
		h.m = req
		if h.m.Mowner == nil || h.m.Usedspace == nil || len(h.m.Mowner) != len(h.m.Usedspace) {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil
		}
		return nil, READ_ROUTINE_NUM
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil
	}
}

func (h *RelationshipSumHandler) Handle() proto.Message {
	sn, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		env.Log.Errorf("%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	count := len(h.m.Mowner)
	var ii int = 0
	for ; ii < count; ii++ {
		dao.SetSpaceSum(sn.ID, h.m.Mowner[ii], h.m.Usedspace[ii])
	}
	return &pkt.VoidResp{}
}

func SumUsedSpace() {
	for {
		time.Sleep(time.Duration(15) * time.Minute)
		m, err := dao.SumRelationship()
		if err == nil {
			if len(m) > 0 {
				mowner := []string{}
				usedspaces := []uint64{}
				for k, v := range m {
					mowner = append(mowner, k)
					usedspaces = append(usedspaces, uint64(v))
				}
				req := &pkt.RelationShipSum{Mowner: mowner, Usedspace: usedspaces}
				AyncRequest(req, -1, 0)
			}
			time.Sleep(time.Duration(15) * time.Minute)
		} else {
			time.Sleep(time.Duration(1) * time.Minute)
		}
	}
}
