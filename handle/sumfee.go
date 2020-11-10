package handle

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var BLK_SUMMER_CH chan *BlockSpaceSum

func StartIterateUser() {
	BLK_SUMMER_CH = make(chan *BlockSpaceSum, net.GetSuperNodeCount()*5)
	for {
		if env.SUM_USER_FEE == 0 && net.IsActive() {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		IterateUser()
		time.Sleep(time.Duration(180) * time.Second)
	}
}

func IterateUser() {
	defer env.TracePanic("[SumUsedSpace]")
	var lastId int32 = 0
	limit := 100
	logrus.Infof("[SumUsedSpace]Start iterate user...\n")
	for {
		us, err := dao.ListUsers(lastId, limit, bson.M{"_id": 1, "nextCycle": 1, "username": 1, "costPerCycle": 1})
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		if len(us) == 0 {
			break
		} else {
			for _, user := range us {
				lastId = user.UserID
				sum := &UserObjectSum{UserID: user.UserID, UsedSpace: new(int64), UserName: user.Username, CostPerCycle: user.CostPerCycle}
				atomic.StoreInt64(sum.UsedSpace, 0)
				sum.IterateObjects()
			}
		}
	}
	logrus.Infof("[SumUsedSpace]Iterate user OK!\n")
}

const BLKID_LIMIT = 500

type UserObjectSum struct {
	UserID       int32
	UserName     string
	UsedSpace    *int64
	CostPerCycle uint64
}

func (me *UserObjectSum) AddUsedSapce(space int64) {
	atomic.AddInt64(me.UsedSpace, space)
}

func (me *UserObjectSum) GetUsedSpace() int64 {
	return atomic.LoadInt64(me.UsedSpace)
}

func (me *UserObjectSum) IterateObjects() {
	for {
		lasttime, err := dao.GetUserSumTime(me.UserID)
		if err != nil {
			time.Sleep(time.Duration(15) * time.Second)
			continue
		} else {
			if time.Now().Unix()*1000-lasttime < int64(env.CostSumCycle) {
				return
			}
			break
		}
	}
	logrus.Infof("[SumFileUsedSpace]Start sum fee,UserID:%d\n", me.UserID)
	wgroup := sync.WaitGroup{}
	limit := net.GetSuperNodeCount() * BLKID_LIMIT
	firstId := primitive.NilObjectID
	m := make(map[int32][]int64)
	for {
		ls, id, err := dao.ListObjects(uint32(me.UserID), firstId, limit)
		if err != nil {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		for _, bs := range ls {
			supid := int32(bs[8])
			vbi := env.BytesToId(bs)
			ids, ok := m[supid]
			if ok {
				if len(ids) >= BLKID_LIMIT {
					bss := &BlockSpaceSum{SuperID: supid, VBIS: ids, WG: &wgroup, UserSum: me}
					BLK_SUMMER_CH <- bss
					wgroup.Add(1)
					go DoBlockSpaceSum()
					m[supid] = []int64{vbi}
				} else {
					m[supid] = append(ids, vbi)
				}
			} else {
				m[supid] = []int64{vbi}
			}
		}
		firstId = id
		if firstId == primitive.NilObjectID {
			break
		}
	}
	size := len(m)
	if size > 0 {
		wgroup.Add(size)
		for k, v := range m {
			bss := &BlockSpaceSum{SuperID: k, VBIS: v, WG: &wgroup, UserSum: me}
			BLK_SUMMER_CH <- bss
			go DoBlockSpaceSum()
		}
	}
	wgroup.Wait()
	me.SetCycleFee()
}

func (me *UserObjectSum) SetCycleFee() {
	cost := CalCycleFee(me.GetUsedSpace())
	logrus.Infof("[SumFileUsedSpace]File statistics completed,UserID:%d,usedspace:%d,cost:%d\n", me.UserID, me.UsedSpace, cost)
	var err error
	if cost > 0 {
		if me.CostPerCycle == cost {
			logrus.Infof("[SumFileUsedSpace]Not need to set costPerCycle,old cost:%d,UserID:%d\n", me.UsedSpace, me.UserID)
		} else {
			err = net.SetHfee(me.UserName, cost)
			if err == nil {
				logrus.Infof("[SumFileUsedSpace]Set costPerCycle:%d,usedspace:%d,UserID:%d\n", cost, me.UsedSpace, me.UserID)
			}
			dao.UpdateUserCost(me.UserID, cost)
		}
	}
	if err == nil {
		dao.SetUserSumTime(me.UserID)
	}
}

type BlockSpaceSum struct {
	SuperID int32
	VBIS    []int64
	WG      *sync.WaitGroup
	UserSum *UserObjectSum
}

func (bss *BlockSpaceSum) ReqBlockUsedSpace() (int64, *pkt.ErrorMessage) {
	sn := net.GetSuperNode(int(bss.SuperID))
	if sn == nil {
		logrus.Errorf("[SumBlockUsedSpace]ERR SNID:%d\n", bss.SuperID)
	}
	msg := &pkt.GetBlockUsedSpace{Id: bss.VBIS}
	if sn.ID == int32(env.SuperNodeID) {
		handler := &BlockUsedSpaceHandler{pkey: sn.PubKey, m: msg}
		res := handler.Handle()
		if resp, ok := res.(*pkt.ErrorMessage); ok {
			return 0, resp
		} else {
			return (res.(*pkt.LongResp)).Value, nil
		}
	} else {
		res, err := net.RequestSN(msg, sn, "", 0, false)
		if err != nil {
			return 0, err
		} else {
			return (res.(*pkt.LongResp)).Value, nil
		}
	}
}

func DoBlockSpaceSum() {
	bss := <-BLK_SUMMER_CH
	if bss == nil {
		return
	}
	defer bss.WG.Done()
	defer env.TracePanic("[SumBlockUsedSpace]")
	var space int64
	for {
		uspace, err := bss.ReqBlockUsedSpace()
		if err != nil {
			logrus.Errorf("[SumBlockUsedSpace]ERR:%d,retry...\n", err.GetCode())
			time.Sleep(time.Duration(60) * time.Second)
		} else {
			space = uspace
			break
		}
	}
	bss.UserSum.AddUsedSapce(space)
	logrus.Infof("[SumBlockUsedSpace]OK,block count:%d,usedspace:%d,snid:%d\n", len(bss.VBIS), space, bss.SuperID)
}

type BlockUsedSpaceHandler struct {
	pkey string
	m    *pkt.GetBlockUsedSpace
}

func (h *BlockUsedSpaceHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.GetBlockUsedSpace)
	if ok {
		h.m = req
		if h.m.Id == nil || len(h.m.Id) == 0 {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		return nil, READ_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *BlockUsedSpaceHandler) Handle() proto.Message {
	_, err := net.AuthSuperNode(h.pkey)
	if err != nil {
		logrus.Errorf("[SumBlockUsedSpace]AuthSuper ERR:%s\n", err)
		return pkt.NewErrorMsg(pkt.INVALID_NODE_ID, err.Error())
	}
	metas, err := dao.GetUsedSpace(h.m.Id)
	if err != nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "")
	}
	space := h.GetUsedSpaceByMap(metas)
	return &pkt.LongResp{Value: space}
}

func (h *BlockUsedSpaceHandler) GetUsedSpaceByMap(metas map[int64]*dao.BlockMeta) int64 {
	var space int64 = 0
	for _, m := range metas {
		if m == nil {
			continue
		}
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
	return space
}

func (h *BlockUsedSpaceHandler) GetUsedSpaceByID(metas map[int64]*dao.BlockMeta) int64 {
	var space int64 = 0
	var count int = 0
	m := make(map[int64]*dao.BlockMeta)
	for _, id := range h.m.Id {
		m[id] = metas[id]
		count++
		if count > 2000 {
			space = space + h.GetUsedSpaceByMap(m)
			count = 0
			m = make(map[int64]*dao.BlockMeta)
		}
	}
	if len(m) > 0 {
		space = space + h.GetUsedSpaceByMap(m)
	}
	return space
}

func (h *BlockUsedSpaceHandler) GetUsedSpace(metas map[int64]*dao.BlockMeta) int64 {
	var space int64 = 0
	for _, id := range h.m.Id {
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
