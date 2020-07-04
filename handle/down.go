package handle

import (
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type DownloadObjectInitHandler struct {
	pkey string
	m    *pkt.DownloadObjectInitReqV2
	user *dao.User
}

func (h *DownloadObjectInitHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.DownloadObjectInitReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.VHW == nil {
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

func (h *DownloadObjectInitHandler) Handle() proto.Message {
	meta := dao.NewObjectMeta(h.user.UserID, h.m.VHW)
	err := meta.GetByVHW()
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	logrus.Infof("[DownloadObj]UID:%d,VNU:%s\n", h.user.UserID, meta.VNU.Hex())
	size := uint32(len(meta.BlockList))
	refs := &pkt.DownloadObjectInitResp_RefList{Count: &size, Refers: meta.BlockList}
	return &pkt.DownloadObjectInitResp{Reflist: refs, Length: &meta.Length}
}

type DownloadFileHandler struct {
	pkey  string
	m     *pkt.DownloadFileReqV2
	user  *dao.User
	verid primitive.ObjectID
}

func (h *DownloadFileHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.DownloadFileReqV2)
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

func (h *DownloadFileHandler) Handle() proto.Message {
	logrus.Infof("[DownloadFile]UID:%d,BucketName:%s,FileName:%s\n", h.user.UserID, *h.m.Bucketname, *h.m.FileName)
	bmeta, err := dao.GetBucketIdFromCache(*h.m.Bucketname, h.user.UserID)
	if err != nil {
		return pkt.NewError(pkt.INVALID_BUCKET_NAME)
	}
	if h.verid == primitive.NilObjectID {
		fmeta := &dao.FileMeta{UserId: h.user.UserID, BucketId: bmeta.BucketId, FileName: *h.m.FileName}
		err = fmeta.GetLastFileMeta(true)
		if err != nil {
			return pkt.NewError(pkt.INVALID_OBJECT_NAME)
		}
		h.verid = fmeta.VersionId
	}
	meta := &dao.ObjectMeta{UserId: h.user.UserID, VNU: h.verid}
	err = meta.GetByVNU()
	if err != nil {
		return pkt.NewError(pkt.INVALID_OBJECT_NAME)
	}
	size := uint32(len(meta.BlockList))
	refs := &pkt.DownloadObjectInitResp_RefList{Count: &size, Refers: meta.BlockList}
	return &pkt.DownloadObjectInitResp{Reflist: refs, Length: &meta.Length}
}

type DownloadBlockInitHandler struct {
	pkey string
	m    *pkt.DownloadBlockInitReqV2
	user *dao.User
}

func (h *DownloadBlockInitHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.DownloadBlockInitReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.VBI == nil {
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

func (h *DownloadBlockInitHandler) Handle() proto.Message {
	logrus.Infof("[DownloadBLK]VBI:%d\n", *h.m.VBI)
	bmeta, err := dao.GetBlockVNF(int64(*h.m.VBI))
	if bmeta == nil {
		return pkt.NewError(pkt.NO_SUCH_BLOCK)
	}
	if bmeta.VNF == 0 {
		dat := dao.GetBlockData(int64(*h.m.VBI))
		return &pkt.DownloadBlockDBResp{Data: dat}
	}
	metas, err := dao.GetShardMetas(int64(*h.m.VBI), int(bmeta.VNF))
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	nodeidsls := []int32{}
	vhfs := make([][]byte, bmeta.VNF)
	nodeids := make([]int32, bmeta.VNF)
	for index, v := range metas {
		vhfs[index] = v.VHF
		nodeids[index] = v.NodeId
		if !IsExistInArray(v.NodeId, nodeidsls) {
			nodeidsls = append(nodeidsls, v.NodeId)
		}
	}
	nodes, err := net.NodeMgr.GetNodes(nodeidsls)
	if err != nil {
		logrus.Errorf("[DownloadBLK]GetNodes ERR:%s\n", err)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	num := len(nodes)
	if num != len(nodeidsls) {
		logrus.Errorf("[DownloadBLK]Some Nodes have been cancelled\n")
	}
	respNodes := make([]*pkt.DownloadBlockInitResp_NList_Ns, num)
	for index, n := range nodes {
		respNodes[index] = &pkt.DownloadBlockInitResp_NList_Ns{
			Id: &n.ID, Nodeid: &n.NodeID, Pubkey: &n.PubKey, Addrs: n.Addrs,
		}
	}
	count := uint32(num)
	nlist := &pkt.DownloadBlockInitResp_NList{Count: &count, Ns: respNodes}
	vhfsize := uint32(len(vhfs))
	vhf := &pkt.DownloadBlockInitResp_VHFS{Count: &vhfsize, VHF: vhfs}
	idsize := uint32(len(nodeids))
	ids := &pkt.DownloadBlockInitResp_Nids{Count: &idsize, Nodeids: nodeids}
	res := &pkt.DownloadBlockInitResp{Nlist: nlist, VNF: new(int32), Vhfs: vhf, Nids: ids, AR: new(int32)}
	*res.AR = int32(bmeta.AR)
	*res.VNF = int32(bmeta.VNF)
	return res
}
