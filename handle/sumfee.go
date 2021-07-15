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
	BLK_SUMMER_CH = make(chan *BlockSpaceSum, env.MAX_SUMFEE_ROUTINE/3)
	for {
		if env.SUM_USER_FEE == 0 && !net.IsActive() {
			time.Sleep(time.Duration(30) * time.Second)
			continue
		}
		IterateUser()
		time.Sleep(time.Duration(180) * time.Second)
	}
}

func IterateUser() {
	defer env.TracePanic("[SumUsedFee]")
	var lastId int32 = 0
	limit := 100
	logrus.Infof("[SumUsedFee]Start iterate user...\n")
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
				sum := &UserObjectSum{UserID: user.UserID, UsedSpace: new(int64), UserName: user.Username, CostPerCycle: uint64(user.CostPerCycle)}
				atomic.StoreInt64(sum.UsedSpace, 0)
				sum.IterateObjects2()
			}
		}
	}
	logrus.Infof("[SumUsedFee]Iterate user OK!\n")
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

func (me *UserObjectSum) IterateObjects2() {
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
	logrus.Infof("[SumUsedFee]Start sum fee,UserID:%d\n", me.UserID)
	limit := 10000
	firstId := primitive.NilObjectID
	for {
		ls, id, err := dao.ListObjects2(uint32(me.UserID), firstId, limit)
		if err != nil {
			logrus.Errorf("[SumUsedFee]UserID %d list object err:%s\n", me.UserID, err)
			time.Sleep(time.Duration(30) * time.Second)
			continue
		} else {
			logrus.Infof("[SumUsedFee]UserID %d list object ok,usedspace %d,time %s\n", me.UserID, ls, id.Timestamp().Format("2006-01-02 15:04:05"))
		}
		me.AddUsedSapce(int64(ls))
		firstId = id
		if firstId == primitive.NilObjectID {
			break
		}
	}
	me.SetCycleFee()
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
	logrus.Infof("[SumUsedFee]Start sum fee,UserID:%d\n", me.UserID)
	wgroup := sync.WaitGroup{}
	limit := net.GetSuperNodeCount() * BLKID_LIMIT
	firstId := primitive.NilObjectID
	m := make(map[int32][]int64)
	for {
		ls, id, err := dao.ListObjects(uint32(me.UserID), firstId, limit)
		if err != nil {
			logrus.Errorf("[SumUsedFee]UserID %d list object err:%s\n", me.UserID, err)
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
					go bss.DoBlockSpaceSum()
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
			go bss.DoBlockSpaceSum()
		}
	}
	wgroup.Wait()
	me.SetCycleFee()
}

func (me *UserObjectSum) SetCycleFee() {
	usedSpace := me.GetUsedSpace()
	cost := CalCycleFee(usedSpace)
	logrus.Infof("[SumUsedFee]File statistics completed,UserID:%d,usedspace:%d,cost:%d\n", me.UserID, usedSpace, cost)
	var err error
	if cost > 0 {
		if me.CostPerCycle == cost {
			logrus.Warnf("[SumUsedFee]Not need to set costPerCycle,old cost:%d,UserID:%d\n", me.CostPerCycle, me.UserID)
		} else {
			num := 0
			for {
				err = net.SetHfee(me.UserName, cost)
				if err != nil {
					num++
					if num > 8 {
						break
					} else {
						time.Sleep(time.Duration(15) * time.Second)
					}
				} else {
					dao.UpdateUserCost(me.UserID, cost)
					logrus.Infof("[SumUsedFee]Set costPerCycle:%d,usedspace:%d,UserID:%d\n", cost, usedSpace, me.UserID)
					break
				}
			}
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
		logrus.Errorf("[SumUsedFee]ERR SNID:%d\n", bss.SuperID)
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

func (bss *BlockSpaceSum) DoFinish() {
	_ = <-BLK_SUMMER_CH
	bss.WG.Done()
	env.TracePanic("[SumUsedFee]")
}

func (bss *BlockSpaceSum) DoBlockSpaceSum() {
	defer bss.DoFinish()
	var space int64
	for {
		uspace, err := bss.ReqBlockUsedSpace()
		if err != nil {
			logrus.Errorf("[SumUsedFee]ERR:%d,retry...\n", err.GetCode())
			time.Sleep(time.Duration(60) * time.Second)
		} else {
			space = uspace
			break
		}
	}
	bss.UserSum.AddUsedSapce(space)
	logrus.Infof("[SumUsedFee]OK,block count:%d,usedspace:%d,snid:%d\n", len(bss.VBIS), space, bss.SuperID)
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
		return nil, SUMFEE_ROUTINE_NUM, nil
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
	space := h.GetUsedSpaceByID(metas)
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
