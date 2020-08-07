package handle

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/dao"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UploadBlockInitHandler struct {
	pkey string
	m    *pkt.UploadBlockInitReqV2
	user *dao.User
	vnu  primitive.ObjectID
}

func (h *UploadBlockInitHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockInitReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Id == nil || h.m.VHP == nil || h.m.Vnu == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UploadBlockInitHandler) Handle() proto.Message {
	logrus.Infof("[UploadBLK]Init %d/%s/%d\n", h.user.UserID, h.vnu.Hex(), *h.m.Id)
	if env.S3Version != "" {
		if h.m.Version == nil || *h.m.Version == "" || bytes.Compare([]byte(*h.m.Version), []byte(env.S3Version)) < 0 {
			errmsg := fmt.Sprintf("[UploadBLK]UID:%d,ERR:TOO_LOW_VERSION?%s\n", h.user.UserID, *h.m.Version)
			logrus.Errorf(errmsg)
			return pkt.NewErrorMsg(pkt.TOO_LOW_VERSION, errmsg)
		}
	}
	n := net.GetBlockSuperNode(h.m.VHP)
	if n.ID != int32(env.SuperNodeID) {
		return pkt.NewErrorMsg(pkt.ILLEGAL_VHP_NODEID, "Invalid request")
	}
	st := uint64(time.Now().Unix())
	if env.DE_DUPLICATION {
		return &pkt.UploadBlockInitResp{StartTime: &st}
	}
	ls, err := dao.GetBlockByVHP(h.m.VHP)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if ls == nil {
		return &pkt.UploadBlockInitResp{StartTime: &st}
	} else {
		size := len(ls)
		vhbs := make([][]byte, size)
		keds := make([][]byte, size)
		ars := make([]int32, size)
		for index, m := range ls {
			vhbs[index] = m.VHB
			keds[index] = m.KED
			ars[index] = int32(m.AR)
		}
		count := uint32(size)
		pbvhbs := &pkt.UploadBlockDupResp_VHBS{Count: &count, VHB: vhbs}
		pbkeds := &pkt.UploadBlockDupResp_KEDS{Count: &count, KED: keds}
		pbars := &pkt.UploadBlockDupResp_ARS{Count: &count, AR: ars}
		return &pkt.UploadBlockDupResp{StartTime: &st, Vhbs: pbvhbs, Keds: pbkeds, Ars: pbars}
	}
}

type UploadBlockDBHandler struct {
	pkey string
	m    *pkt.UploadBlockDBReqV2
	user *dao.User
	vnu  primitive.ObjectID
}

func (h *UploadBlockDBHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockDBReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Id == nil || h.m.Vnu == nil || h.m.OriginalSize == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.Data == nil || len(h.m.Data) > env.PL2 {
			return pkt.NewError(pkt.TOO_BIG_BLOCK), nil, nil
		}
		b := &codec.EncryptedBlock{}
		b.Data = h.m.Data
		err := b.MakeVHB()
		if err != nil || h.m.VHB == nil || len(h.m.VHB) != 16 || !bytes.Equal(b.VHB, h.m.VHB) {
			return pkt.NewError(pkt.INVALID_VHB), nil, nil
		}
		if h.m.VHP == nil || len(h.m.VHP) != 32 {
			return pkt.NewError(pkt.INVALID_VHP), nil, nil
		}
		if h.m.KEU == nil || len(h.m.KEU) != 32 {
			return pkt.NewError(pkt.INVALID_KEU), nil, nil
		}
		if h.m.KED == nil || len(h.m.KED) != 32 {
			return pkt.NewError(pkt.INVALID_KED), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UploadBlockDBHandler) Handle() proto.Message {
	logrus.Infof("[UploadBLK]Save block %d/%s/%d to DB...\n", h.user.UserID, h.vnu.Hex(), *h.m.Id)
	n := net.GetBlockSuperNode(h.m.VHP)
	if n.ID != int32(env.SuperNodeID) {
		return pkt.NewErrorMsg(pkt.ILLEGAL_VHP_NODEID, "Invalid request")
	}
	vbi := dao.GenerateBlockID(1)
	meta, err := dao.GetBlockByVHP_VHB(h.m.VHP, h.m.VHB)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if meta != nil {
		if !bytes.Equal(meta.KED, h.m.KED) {
			logrus.Errorf("[UploadBLK]Block meta duplicate writing.\n")
			return pkt.NewError(pkt.SERVER_ERROR)
		} else {
			vbi = meta.VBI
		}
	}
	err = dao.SaveBlockData(vbi, h.m.Data)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if meta == nil {
		meta = &dao.BlockMeta{VBI: vbi, VHP: h.m.VHP, VHB: h.m.VHB, KED: h.m.KED, VNF: 0, NLINK: 1, AR: codec.AR_DB_MODE}
		err = dao.SaveBlockMeta(meta)
		if err != nil {
			return pkt.NewError(pkt.SERVER_ERROR)
		}
	}
	ref := &pkt.Refer{VBI: vbi, SuperID: uint8(env.SuperNodeID), OriginalSize: int64(*h.m.OriginalSize),
		RealSize: int32(len(h.m.Data)), KEU: h.m.KEU, KeyNumber: int16(*h.m.KeyNumber), Id: int16(*h.m.Id)}
	vnustr := h.vnu.Hex()
	usedSpace := uint64(env.PCM)
	saveObjectMetaReq := &pkt.SaveObjectMetaReq{UserID: &h.user.UserID, VNU: &vnustr,
		Refer: ref.Bytes(), UsedSpace: &usedSpace, Mode: new(bool)}
	*saveObjectMetaReq.Mode = false
	res, perr := SaveObjectMeta(saveObjectMetaReq, ref, h.vnu)
	if perr != nil {
		return perr
	} else {
		if saveObjectMetaResp, ok := res.(*pkt.SaveObjectMetaResp); ok {
			if saveObjectMetaResp.Exists != nil && *saveObjectMetaResp.Exists == true {
				logrus.Warnf("[UploadBLK]Block %d/%s/%d has been uploaded.\n", h.user.UserID, h.vnu.Hex(), *h.m.Id)
			}
		}
	}
	return &pkt.VoidResp{}
}

type UploadBlockDupHandler struct {
	pkey string
	m    *pkt.UploadBlockDupReqV2
	user *dao.User
	vnu  primitive.ObjectID
}

func (h *UploadBlockDupHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockDupReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Vnu == nil || h.m.Id == nil || h.m.OriginalSize == nil || h.m.RealSize == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.KEU == nil || len(h.m.KEU) != 32 {
			return pkt.NewError(pkt.INVALID_KEU), nil, nil
		}
		if h.m.VHP == nil || len(h.m.VHP) != 32 {
			return pkt.NewError(pkt.INVALID_VHP), nil, nil
		}
		if h.m.VHB == nil || len(h.m.VHB) != 16 {
			return pkt.NewError(pkt.INVALID_VHB), nil, nil
		}
		h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UploadBlockDupHandler) Handle() proto.Message {
	logrus.Infof("[UploadBLK]/%d/%s/%d exist...\n", h.user.UserID, h.vnu.Hex(), *h.m.Id)
	meta, _ := dao.GetBlockByVHP_VHB(h.m.VHP, h.m.VHB)
	if meta == nil {
		return pkt.NewError(pkt.NO_SUCH_BLOCK)
	}
	usedSpace := env.PCM
	if meta.AR != codec.AR_DB_MODE {
		usedSpace = env.PFL * uint64(meta.VNF) * uint64(env.Space_factor) / 100
	}
	vnustr := h.vnu.Hex()
	ref := &pkt.Refer{VBI: meta.VBI, SuperID: uint8(env.SuperNodeID), OriginalSize: int64(*h.m.OriginalSize),
		RealSize: int32(*h.m.RealSize), KEU: h.m.KEU, KeyNumber: int16(*h.m.KeyNumber), Id: int16(*h.m.Id)}
	saveObjectMetaReq := &pkt.SaveObjectMetaReq{UserID: &h.user.UserID, VNU: &vnustr,
		Refer: ref.Bytes(), UsedSpace: &usedSpace, Mode: new(bool)}
	*saveObjectMetaReq.Mode = false
	res, perr := SaveObjectMeta(saveObjectMetaReq, ref, h.vnu)
	if perr != nil {
		return perr
	} else {
		if saveObjectMetaResp, ok := res.(*pkt.SaveObjectMetaResp); ok {
			if saveObjectMetaResp.Exists != nil && *saveObjectMetaResp.Exists == true {
				logrus.Warnf("[UploadBLK]Block %d/%s/%d has been uploaded.\n", h.user.UserID, h.vnu.Hex(), *h.m.Id)
			} else {
				dao.IncBlockNlinkCount()
			}
		}
	}
	return &pkt.VoidResp{}
}

type UploadBlockEndHandler struct {
	pkey string
	m    *pkt.UploadBlockEndReqV2
	user *dao.User
	vnu  primitive.ObjectID
}

func (h *UploadBlockEndHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockEndReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Oklist == nil || len(h.m.Oklist) == 0 || len(h.m.Oklist) > env.Max_Shard_Count+env.Default_PND {
			return pkt.NewError(pkt.TOO_MANY_SHARDS), nil, nil
		}
		if h.m.VHB == nil || len(h.m.VHB) != 16 {
			return pkt.NewError(pkt.INVALID_VHB), nil, nil
		}
		if h.m.VHP == nil || len(h.m.VHP) != 32 {
			return pkt.NewError(pkt.INVALID_VHP), nil, nil
		}
		if h.m.KEU == nil || len(h.m.KEU) != 32 {
			return pkt.NewError(pkt.INVALID_KEU), nil, nil
		}
		if h.m.KED == nil || len(h.m.KED) != 32 {
			return pkt.NewError(pkt.INVALID_KED), nil, nil
		}
		if h.m.Vnu == nil || h.m.Id == nil || h.m.OriginalSize == nil || h.m.RealSize == nil || h.m.AR == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UploadBlockEndHandler) Handle() proto.Message {
	logrus.Debugf("[UploadBLK]Receive UploadBlockEnd request:/%s/%d\n", h.vnu.Hex(), *h.m.Id)
	startTime := time.Now()
	inblkids := NotInBlackList(h.m.Oklist, h.user.UserID)
	if inblkids != nil && len(inblkids) > 0 {
		txt, _ := json.Marshal(inblkids)
		if txt != nil {
			return pkt.NewErrorMsg(pkt.DN_IN_BLACKLIST, string(txt))
		} else {
			return pkt.NewError(pkt.DN_IN_BLACKLIST)
		}
	}
	shardcount := len(h.m.Oklist)
	vbi := dao.GenerateBlockID(shardcount)
	meta, err := dao.GetBlockByVHP_VHB(h.m.VHP, h.m.VHB)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if meta != nil {
		if !bytes.Equal(meta.KED, h.m.KED) {
			logrus.Warnf("[UploadBLK]Block meta duplicate writing.\n")
		} else {
			vbi = meta.VBI
		}
	}
	shardMetas := make([]*dao.ShardMeta, shardcount)
	nodeidsls := []int32{}
	for _, v := range h.m.Oklist {
		if v.SHARDID == nil || *v.SHARDID >= int32(shardcount) || v.NODEID == nil || v.VHF == nil || v.DNSIGN == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:OkList")
		}
		shardMetas[*v.SHARDID] = &dao.ShardMeta{VFI: int64(*v.SHARDID), NodeId: *v.NODEID, VHF: v.VHF}
		if !IsExistInArray(int32(*v.NODEID), nodeidsls) {
			nodeidsls = append(nodeidsls, int32(*v.NODEID))
		}
	}
	msgerr := VerifyShards(shardMetas, nodeidsls, vbi, shardcount, *h.m.AR, h.m.VHB)
	if msgerr != nil {
		return msgerr
	}
	if meta == nil {
		meta = &dao.BlockMeta{VBI: vbi, VHP: h.m.VHP, VHB: h.m.VHB, KED: h.m.KED,
			VNF: int16(shardcount), NLINK: 1, AR: int16(*h.m.AR)}
		dao.SaveBlockMeta(meta)
	}
	logrus.Debugf("[UploadBLK]/%s/%d OK,take times %d ms\n", h.vnu.Hex(), *h.m.Id, time.Now().Sub(startTime).Milliseconds())
	startTime = time.Now()
	usedSpace := uint64(env.PFL * shardcount)
	vnustr := h.vnu.Hex()
	ref := &pkt.Refer{VBI: meta.VBI, SuperID: uint8(env.SuperNodeID), OriginalSize: *h.m.OriginalSize,
		RealSize: *h.m.RealSize, KEU: h.m.KEU, KeyNumber: int16(*h.m.KeyNumber), Id: int16(*h.m.Id)}
	saveObjectMetaReq := &pkt.SaveObjectMetaReq{UserID: &h.user.UserID, VNU: &vnustr,
		Refer: ref.Bytes(), UsedSpace: &usedSpace, Mode: new(bool)}
	*saveObjectMetaReq.Mode = false
	res, perr := SaveObjectMeta(saveObjectMetaReq, ref, h.vnu)
	if perr != nil {
		logrus.Errorf("[UploadBLK]Save object refer:/%s/%d ERR:%s\n", h.vnu.Hex(), *h.m.Id, perr.Msg)
		return perr
	} else {
		if saveObjectMetaResp, ok := res.(*pkt.SaveObjectMetaResp); ok {
			if saveObjectMetaResp.Exists != nil && *saveObjectMetaResp.Exists == true {
				logrus.Warnf("[UploadBLK]Block %d/%s/%d has been uploaded.\n", h.user.UserID, h.vnu.Hex(), *h.m.Id)
			}
		}
	}
	logrus.Infof("[UploadBLK]Save object refer:/%s/%d OK,take times %d ms\n", h.vnu.Hex(), *h.m.Id, time.Now().Sub(startTime).Milliseconds())
	ip := net.GetSelfIp()
	return &pkt.UploadBlockEndResp{Host: &ip, VBI: &vbi}
}

func VerifyShards(shardMetas []*dao.ShardMeta, nodeidsls []int32, vbi int64, count int, AR int32, VHB []byte) *pkt.ErrorMessage {
	for _, v := range shardMetas {
		if v == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:OkList Missing")
		}
		v.VFI = vbi + v.VFI
	}
	nodes, err := GetNodes(nodeidsls)
	if err != nil {
		logrus.Errorf("[UploadBLK]GetNodes ERR:%s\n", err)
		return pkt.NewError(pkt.NO_ENOUGH_NODE)
	}
	if len(nodes) != len(nodeidsls) {
		logrus.Errorf("[UploadBLK]Some Nodes have been cancelled\n")
		return pkt.NewError(pkt.NO_ENOUGH_NODE)
	}
	num := count / len(nodeidsls)
	if count%len(nodeidsls) > 0 {
		num = num + 1
	}
	if num > env.ShardNumPerNode {
		logrus.Warnf("[UploadBLK]Number of nodes less than %d/%d\n", len(nodeidsls), count)
		return pkt.NewError(pkt.NO_ENOUGH_NODE)
	}
	md5Digest := md5.New()
	for index, m := range shardMetas {
		if AR != codec.AR_COPY_MODE {
			md5Digest.Write(m.VHF)
		} else {
			if index == 0 {
				md5Digest.Write(m.VHF)
			}
		}
		if !verifySign(m, nodes) {
			return pkt.NewError(pkt.INVALID_SIGNATURE)
		}
	}
	vhb := md5Digest.Sum(nil)
	if !bytes.Equal(vhb, VHB) {
		return pkt.NewError(pkt.INVALID_VHB)
	}
	err = dao.SaveShardMetas(shardMetas)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	return nil
}

func verifySign(meta *dao.ShardMeta, node []*net.Node) bool {
	return true
}

type UploadBlockEndSyncHandler struct {
	pkey string
	m    *pkt.UploadBlockEndSyncReqV2
	user *dao.User
	vnu  primitive.ObjectID
}

func (h *UploadBlockEndSyncHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockEndSyncReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.VBI == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Oklist == nil || len(h.m.Oklist) == 0 || len(h.m.Oklist) > env.Max_Shard_Count+env.Default_PND {
			return pkt.NewError(pkt.TOO_MANY_SHARDS), nil, nil
		}
		if h.m.VHB == nil || len(h.m.VHB) != 16 {
			return pkt.NewError(pkt.INVALID_VHB), nil, nil
		}
		if h.m.VHP == nil || len(h.m.VHP) != 32 {
			return pkt.NewError(pkt.INVALID_VHP), nil, nil
		}
		if h.m.KEU == nil || len(h.m.KEU) != 32 {
			return pkt.NewError(pkt.INVALID_KEU), nil, nil
		}
		if h.m.KED == nil || len(h.m.KED) != 32 {
			return pkt.NewError(pkt.INVALID_KED), nil, nil
		}
		if h.m.Vnu == nil || h.m.Id == nil || h.m.OriginalSize == nil || h.m.RealSize == nil || h.m.AR == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		if h.m.Vnu.Timestamp == nil || h.m.Vnu.MachineIdentifier == nil || h.m.Vnu.ProcessIdentifier == nil || h.m.Vnu.Counter == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.vnu = pkt.NewObjectId(*h.m.Vnu.Timestamp, *h.m.Vnu.MachineIdentifier, *h.m.Vnu.ProcessIdentifier, *h.m.Vnu.Counter)
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UploadBlockEndSyncHandler) Handle() proto.Message {
	logrus.Debugf("[UploadBLK]Receive UploadBlockEndSync request:/%s/%d\n", h.vnu.Hex(), *h.m.Id)
	startTime := time.Now()
	vbi := *h.m.VBI
	meta, err := dao.GetBlockById(vbi)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if meta != nil {
		return &pkt.VoidResp{}
	}
	shardcount := len(h.m.Oklist)
	shardMetas := make([]*dao.ShardMeta, shardcount)
	nodeidsls := []int32{}
	for _, v := range h.m.Oklist {
		if v.SHARDID == nil || *v.SHARDID >= int32(shardcount) || v.NODEID == nil || v.VHF == nil || v.DNSIGN == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:OkList")
		}
		shardMetas[*v.SHARDID] = &dao.ShardMeta{VFI: int64(*v.SHARDID), NodeId: *v.NODEID, VHF: v.VHF}
		if !IsExistInArray(int32(*v.NODEID), nodeidsls) {
			nodeidsls = append(nodeidsls, int32(*v.NODEID))
		}
	}
	msgerr := VerifyShards(shardMetas, nodeidsls, vbi, shardcount, *h.m.AR, h.m.VHB)
	if msgerr != nil {
		return msgerr
	}
	meta = &dao.BlockMeta{VBI: vbi, VHP: h.m.VHP, VHB: h.m.VHB, KED: h.m.KED,
		VNF: int16(shardcount), NLINK: 1, AR: int16(*h.m.AR)}
	dao.SaveBlockMeta(meta)
	logrus.Debugf("[UploadBLK]Upload block Sync:/%s/%d OK,take times %d ms\n", h.vnu.Hex(), *h.m.Id, time.Now().Sub(startTime).Milliseconds())
	startTime = time.Now()
	usedSpace := uint64(env.PFL * shardcount)
	vnustr := h.vnu.Hex()
	ref := &pkt.Refer{VBI: meta.VBI, SuperID: uint8(env.SuperNodeID), OriginalSize: *h.m.OriginalSize,
		RealSize: *h.m.RealSize, KEU: h.m.KEU, KeyNumber: int16(*h.m.KeyNumber), Id: int16(*h.m.Id)}
	saveObjectMetaReq := &pkt.SaveObjectMetaReq{UserID: &h.user.UserID, VNU: &vnustr,
		Refer: ref.Bytes(), UsedSpace: &usedSpace, Mode: new(bool)}
	*saveObjectMetaReq.Mode = false
	res, perr := SaveObjectMeta(saveObjectMetaReq, ref, h.vnu)
	if perr != nil {
		logrus.Infof("[UploadBLK]Sync Save object refer:/%s/%d ERR:%s\n", h.vnu.Hex(), *h.m.Id, perr.Msg)
		return perr
	} else {
		if saveObjectMetaResp, ok := res.(*pkt.SaveObjectMetaResp); ok {
			if saveObjectMetaResp.Exists != nil && *saveObjectMetaResp.Exists == true {
				logrus.Warnf("[UploadBLK]Block %d/%s/%d has been uploaded.\n", h.user.UserID, h.vnu.Hex(), *h.m.Id)
			}
		}
	}
	logrus.Infof("[UploadBLK]Sync Save object refer:/%s/%d OK,take times %d ms\n", h.vnu.Hex(), *h.m.Id, time.Now().Sub(startTime).Milliseconds())
	return &pkt.VoidResp{}
}
