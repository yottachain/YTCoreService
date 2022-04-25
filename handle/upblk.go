package handle

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
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

type CheckBlockDupHandler struct {
	pkey string
	m    *pkt.CheckBlockDupReq
	user *dao.User
}

func (h *CheckBlockDupHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.CheckBlockDupReq)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.VHP == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		return nil, READ_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *CheckBlockDupHandler) Handle() proto.Message {
	logrus.Debugf("[CheckBlockDup]User %d\n", h.user.UserID)
	return CheckBlockDup(h.m.VHP)
}

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
		//compare
		reqInterface := reflect.TypeOf(*req)
		logrus.Infof("[wangjun][UploadBLK][SetMessage] req=%+v\n", *req)
		_, result := reqInterface.FieldByName("CompareFlag")
		if !result || req.CompareFlag == nil {
			return pkt.NewErrorMsg(pkt.TOO_LOW_VERSION, "CompareFlag check failed"), nil, nil
		}
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
			v := "NULL"
			if h.m.Version != nil {
				v = *h.m.Version
			}
			errmsg := fmt.Sprintf("UID:%d,ERR:TOO_LOW_VERSION?%s", h.user.UserID, v)
			logrus.Errorf("[UploadBLK]%s\n", errmsg)
			return pkt.NewErrorMsg(pkt.TOO_LOW_VERSION, errmsg)
		}
	}
	return CheckBlockDup(h.m.VHP)
}

func CheckBlockDup(vhp []byte) proto.Message {
	n := net.GetBlockSuperNode(vhp)
	if n.ID != int32(env.SuperNodeID) {
		return pkt.NewErrorMsg(pkt.ILLEGAL_VHP_NODEID, "Invalid request")
	}
	st := uint64(time.Now().Unix())
	if env.DE_DUPLICATION {
		vbi := uint64(dao.GenerateBlockID(env.Max_Shard_Count + env.Default_PND))
		return &pkt.UploadBlockInitResp{StartTime: &vbi}
	}
	ls, err := dao.GetBlockByVHP(vhp)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if ls == nil {
		vbi := uint64(dao.GenerateBlockID(env.Max_Shard_Count + env.Default_PND))
		return &pkt.UploadBlockInitResp{StartTime: &vbi}
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
	pkey        string
	m           *pkt.UploadBlockDBReqV2
	user        *dao.User
	vnu         primitive.ObjectID
	storeNumber int32
}

func (h *UploadBlockDBHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockDBReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil || h.m.Id == nil || h.m.Vnu == nil || h.m.OriginalSize == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		sign, num := GetStoreNumber(*h.m.SignData, int32(*h.m.KeyNumber))
		*h.m.SignData = sign
		h.storeNumber = num
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
		RealSize: int32(len(h.m.Data)), KEU: h.m.KEU, KeyNumber: int16(h.storeNumber), Id: int16(*h.m.Id)}
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
	pkey        string
	m           *pkt.UploadBlockDupReqV2
	user        *dao.User
	vnu         primitive.ObjectID
	storeNumber int32
}

func (h *UploadBlockDupHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockDupReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		sign, num := GetStoreNumber(*h.m.SignData, int32(*h.m.KeyNumber))
		*h.m.SignData = sign
		h.storeNumber = num
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
		usedSpace = env.PFL * uint64(meta.VNF+1) * uint64(env.Space_factor) / 100
	}
	vnustr := h.vnu.Hex()
	ref := &pkt.Refer{VBI: meta.VBI, SuperID: uint8(env.SuperNodeID), OriginalSize: int64(*h.m.OriginalSize),
		RealSize: int32(*h.m.RealSize), KEU: h.m.KEU, KeyNumber: int16(h.storeNumber), Id: int16(*h.m.Id)}
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
				dao.INCBlockNLINK(meta)
			}
		}
	}
	return &pkt.VoidResp{}
}

func GetStoreNumber(signdata string, signnumber int32) (string, int32) {
	type SignData struct {
		Number int32
		Sign   string
	}
	data := &SignData{}
	err := json.Unmarshal([]byte(signdata), &data)
	if err != nil {
		return signdata, signnumber
	} else {
		return data.Sign, data.Number
	}
}

type UploadBlockEndHandler struct {
	pkey        string
	m           *pkt.UploadBlockEndReqV2
	user        *dao.User
	vnu         primitive.ObjectID
	storeNumber int32
}

func (h *UploadBlockEndHandler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockEndReqV2)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		sign, num := GetStoreNumber(*h.m.SignData, *h.m.KeyNumber)
		*h.m.SignData = sign
		h.storeNumber = num
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
		jsonstr := ""
		if txt != nil {
			jsonstr = string(txt)
		}
		logrus.Warnf("[UploadBLK][%d]DN_IN_BLACKLIST ERR:%s\n", h.user.UserID, jsonstr)
		return pkt.NewErrorMsg(pkt.DN_IN_BLACKLIST, jsonstr)
	}
	shardcount := len(h.m.Oklist)
	var vbi int64
	if h.m.Vbi == nil {
		vbi = dao.GenerateBlockID(shardcount)
	} else {
		vbi = *h.m.Vbi
	}
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
	signs := make([][]string, shardcount)
	nodeidsls := []int32{}
	//compare shards
	var compareShards []*pkt.CompareShardReq_Shards
	for n, v := range h.m.Oklist {
		if v.SHARDID == nil || *v.SHARDID >= int32(shardcount) || v.NODEID == nil || v.VHF == nil || v.DNSIGN == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:OkList")
		}
		signs[*v.SHARDID] = []string{*v.DNSIGN, ""}
		shardMetas[*v.SHARDID] = &dao.ShardMeta{VFI: int64(*v.SHARDID), NodeId: *v.NODEID, VHF: v.VHF}
		if !env.IsExistInArray(int32(*v.NODEID), nodeidsls) {
			nodeidsls = append(nodeidsls, int32(*v.NODEID))
		}
		compareShard := &pkt.CompareShardReq_Shards{NodeId: v.NODEID, Seq: h.m.Shardseqlist[n].Seq, VHF: v.VHF}
		compareShards = append(compareShards, compareShard)
	}
	//save compare shard
	body := url.Values{}
	s, _ := json.Marshal(compareShards)
	body.Add("shards", string(s))
	resp, err := http.PostForm(env.SAVE_COMPARE_SHARD_URL, body)
	if err != nil {
		logrus.Errorf("[UploadBLK]SaveCompareShard request ERR:%s\n", err)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logrus.Errorf("[UploadBLK]SaveCompareShard resp ReadAll ERR:%s\n", err)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if resp.StatusCode != http.StatusOK {
		logrus.Errorf("[UploadBLK]SaveCompareShard failed. respbody:%s\n", string(respBody))
		return pkt.NewError(pkt.SERVER_ERROR)
	}

	msgerr := VerifyShards(shardMetas, signs, nodeidsls, vbi, *h.m.AR, h.m.VHB, false)
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
		RealSize: *h.m.RealSize, KEU: h.m.KEU, KeyNumber: int16(h.storeNumber), Id: int16(*h.m.Id)}
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
	ip := net.SelfIP
	return &pkt.UploadBlockEndResp{Host: &ip, VBI: &vbi}
}

func VerifyShards(shardMetas []*dao.ShardMeta, signs [][]string, nodeidsls []int32, vbi int64, AR int32, VHB []byte, lrc2 bool) *pkt.ErrorMessage {
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
	shdnum := len(shardMetas)
	nodenum := len(nodeidsls)
	num := 0
	if lrc2 {
		num = (shdnum * 2) / nodenum
		if (shdnum*2)%nodenum > 0 {
			num = num + 1
		}
	} else {
		num = shdnum / nodenum
		if shdnum%nodenum > 0 {
			num = num + 1
		}
	}
	if num > env.ShardNumPerNode {
		logrus.Warnf("[UploadBLK]Number of nodes less than %d/%d\n", nodenum, shdnum)
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
		if !verifySign(m, signs, nodes) {
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
	err = saveShardCount(vbi, shardMetas, lrc2)
	if err != nil {
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	return nil
}

func saveShardCount(vbi int64, ls []*dao.ShardMeta, lrc2 bool) error {
	m := make(map[int32]int16)
	for _, shard := range ls {
		num, ok := m[shard.NodeId]
		if ok {
			m[shard.NodeId] = num + 1
		} else {
			m[shard.NodeId] = 1
		}
		if lrc2 {
			num, ok = m[shard.NodeId2]
			if ok {
				m[shard.NodeId2] = num + 1
			} else {
				m[shard.NodeId2] = 1
			}
		}
	}
	bs := dao.ToBytes(m)
	return dao.SaveNodeShardCount(vbi, bs)
}

func verifySign(meta *dao.ShardMeta, signs [][]string, node []*net.Node) bool {
	return true
}

type UploadBlockEndV3Handler struct {
	pkey        string
	m           *pkt.UploadBlockEndReqV3
	user        *dao.User
	vnu         primitive.ObjectID
	storeNumber int32
}

func (h *UploadBlockEndV3Handler) SetMessage(pubkey string, msg proto.Message) (*pkt.ErrorMessage, *int32, *int32) {
	h.pkey = pubkey
	req, ok := msg.(*pkt.UploadBlockEndReqV3)
	if ok {
		h.m = req
		if h.m.UserId == nil || h.m.SignData == nil || h.m.KeyNumber == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		sign, num := GetStoreNumber(*h.m.SignData, *h.m.KeyNumber)
		*h.m.SignData = sign
		h.storeNumber = num
		h.user = dao.GetUserCache(int32(*h.m.UserId), int(*h.m.KeyNumber), *h.m.SignData)
		if h.user == nil {
			return pkt.NewError(pkt.INVALID_SIGNATURE), nil, nil
		}
		if h.m.Oklist == nil || len(h.m.Oklist) == 0 || len(h.m.Oklist) > (env.Max_Shard_Count+env.Default_PND)*2 {
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
		if h.m.VNU == nil || h.m.Id == nil || h.m.OriginalSize == nil || h.m.RealSize == nil || h.m.AR == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		vnu, err := primitive.ObjectIDFromHex(*h.m.VNU)
		if err != nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:Null value"), nil, nil
		}
		h.vnu = vnu
		return nil, WRITE_ROUTINE_NUM, nil
	} else {
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request"), nil, nil
	}
}

func (h *UploadBlockEndV3Handler) Handle() proto.Message {
	logrus.Debugf("[UploadBLK]Receive UploadBlockEndV3 request:/%s/%d\n", h.vnu.Hex(), *h.m.Id)
	startTime := time.Now()
	inblkids := NotInBlackListV3(h.m.Oklist, h.user.UserID)
	if inblkids != nil && len(inblkids) > 0 {
		txt, _ := json.Marshal(inblkids)
		jsonstr := ""
		if txt != nil {
			jsonstr = string(txt)
		}
		logrus.Warnf("[UploadBLK][%d]DN_IN_BLACKLIST ERR:%s\n", h.user.UserID, jsonstr)
		return pkt.NewErrorMsg(pkt.DN_IN_BLACKLIST, jsonstr)
	}
	shardcount := len(h.m.Oklist)
	var vbi int64
	if h.m.Vbi == nil {
		vbi = dao.GenerateBlockID(shardcount)
	} else {
		vbi = *h.m.Vbi
	}
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
	signs := make([][]string, shardcount)
	nodeidsls := []int32{}
	//compare shards lrc2
	var compareShards []*pkt.CompareShardReq_Shards
	for n, v := range h.m.Oklist {
		if v.SHARDID == nil || *v.SHARDID >= int32(shardcount) || v.NODEID == nil || v.NODEID2 == nil || v.VHF == nil || v.DNSIGN == nil || v.DNSIGN2 == nil {
			return pkt.NewErrorMsg(pkt.INVALID_ARGS, "Invalid request:OkList")
		}
		shardMetas[*v.SHARDID] = &dao.ShardMeta{VFI: int64(*v.SHARDID), NodeId: *v.NODEID, NodeId2: *v.NODEID2, VHF: v.VHF}
		signs[*v.SHARDID] = []string{*v.DNSIGN, *v.DNSIGN2}
		if !env.IsExistInArray(int32(*v.NODEID), nodeidsls) {
			nodeidsls = append(nodeidsls, int32(*v.NODEID))
		}
		if !env.IsExistInArray(int32(*v.NODEID2), nodeidsls) {
			nodeidsls = append(nodeidsls, int32(*v.NODEID2))
		}
		//compare shard lrc2
		compareShards = append(compareShards, &pkt.CompareShardReq_Shards{NodeId: v.NODEID, Seq: h.m.Shardseqlist[n].Seq, VHF: v.VHF})
		compareShards = append(compareShards, &pkt.CompareShardReq_Shards{NodeId: v.NODEID2, Seq: h.m.Shardseqlist2[n].Seq, VHF: v.VHF})
	}

	//save compare shard
	body := url.Values{}
	s, _ := json.Marshal(compareShards)
	body.Add("shards", string(s))
	resp, err := http.PostForm(env.SAVE_COMPARE_SHARD_URL, body)
	if err != nil {
		logrus.Errorf("[UploadBLK]SaveCompareShard request ERR:%s\n", err)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logrus.Errorf("[UploadBLK]SaveCompareShard resp ReadAll ERR:%s\n", err)
		return pkt.NewError(pkt.SERVER_ERROR)
	}
	if resp.StatusCode != http.StatusOK {
		logrus.Errorf("[UploadBLK]SaveCompareShard failed. respbody:%s\n", string(respBody))
		return pkt.NewError(pkt.SERVER_ERROR)
	}

	msgerr := VerifyShards(shardMetas, signs, nodeidsls, vbi, *h.m.AR, h.m.VHB, true)
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
	usedSpace := uint64(env.PFL * shardcount * 2)
	vnustr := h.vnu.Hex()
	ref := &pkt.Refer{VBI: meta.VBI, SuperID: uint8(env.SuperNodeID), OriginalSize: *h.m.OriginalSize,
		RealSize: *h.m.RealSize, KEU: h.m.KEU, KeyNumber: int16(h.storeNumber), Id: int16(*h.m.Id)}
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
	ip := net.SelfIP
	return &pkt.UploadBlockEndResp{Host: &ip, VBI: &vbi}
}
