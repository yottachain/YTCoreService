package handle

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

func StartSumUsedSpace() {
	for {
		if !net.IsActive() {
			time.Sleep(time.Duration(15) * time.Minute)
			continue
		}
		time.Sleep(time.Duration(15) * time.Minute)
		SumUsedSpace()
	}
}

func SumUsedSpace() {
	defer CatchError("RelationshipSum")
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

type RelationshipSumHandler struct {
	pkey string
	m    *pkt.RelationShipSum
}

func (h *RelationshipSumHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.RelationShipSum)
	if ok {
		h.m = req
		if h.m.Mowner == nil || h.m.Usedspace == nil || len(h.m.Mowner) != len(h.m.Usedspace) {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, READ_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *RelationshipSumHandler) Handle() proto.Message {
	sn, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[RelationshipSum]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	count := len(h.m.Mowner)
	var ii int = 0
	for ; ii < count; ii++ {
		dao.SetSpaceSum(sn.ID, h.m.Mowner[ii], h.m.Usedspace[ii])
	}
	return &pkt.VoidResp{}
}
