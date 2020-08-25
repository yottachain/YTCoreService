package handle

import (
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

var DN_Black_List atomic.Value
var QUERY_ARGS = []int32{-1}

func StartDNBlackListCheck() {
	for {
		err := Query()
		if err != nil {
			time.Sleep(time.Duration(15) * time.Second)
		} else {
			time.Sleep(time.Duration(60*3) * time.Second)
		}
	}
}

func NotInBlackList(oklist []*pkt.UploadBlockEndReqV2_OkList, uid int32) []int32 {
	v := DN_Black_List.Load()
	if v == nil {
		return nil
	}
	ids := v.([]int32)
	var inblackids []int32
	for _, req := range oklist {
		if req.NODEID == nil {
			logrus.Error("[UploadBLK]NodeId is nil,UserId:%d.\n", uid)
			continue
		}
		if env.IsExistInArray(*req.NODEID, ids) {
			logrus.Warnf("[UploadBLK]DN_IN_BLACKLIST ERR,NodeId:%d,UserId:%d.\n", *req.NODEID, uid)
			inblackids = append(inblackids, *req.NODEID)
		}
	}
	return inblackids
}

func Query() error {
	defer env.TracePanic()
	nodes, err := net.NodeMgr.GetNodes(QUERY_ARGS)
	if err != nil {
		return err
	}
	if nodes == nil || len(nodes) == 0 {
		DN_Black_List.Store([]int32{})
		return nil
	}
	ids := make([]int32, len(nodes))
	for index, n := range nodes {
		ids[index] = n.ID
	}
	DN_Black_List.Store(ids)
	return nil
}
