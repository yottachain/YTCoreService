package api

import (
	"errors"
	"fmt"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCoreService/sgx"
)

func AddPublicKey(username, pubkey string) (uint32, error) {
	c := GetClientByName(username)
	if c == nil {
		return 0, fmt.Errorf("user '%s' not registered", username)
	}
	KUEp := base58.Decode(pubkey)
	if KUEp == nil || len(KUEp) != 37 {
		return 0, fmt.Errorf("pubkey '%s' format error", pubkey)
	}
	cc := GetClient(pubkey)
	if cc != nil {
		for k, v := range cc.KeyMap {
			if v.PublicKey == pubkey {
				return k, nil
			}
		}
	} else {
		return addPubkey(c, username, pubkey)
	}
	return 0, fmt.Errorf("user '%s' add pubkey failed", username)
}

func addPubkey(c *Client, username, pubkey string) (uint32, error) {
	req := &pkt.RegUserReqV3{VersionId: &env.Version, Username: &username, PubKey: []string{pubkey}}
	res, err := net.RequestSN(req)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' add pubkey failed!%s", username, pkt.ToError(err))
		logrus.Errorf("[AddPubkey]%s\n", emsg)
		return 0, errors.New(emsg)
	} else {
		resp, ok := res.(*pkt.RegUserRespV2)
		if ok {
			if resp.UserId != nil && resp.KeyNumber != nil &&
				len(resp.KeyNumber) == 1 && resp.KeyNumber[0] >= 0 {
				return uint32(resp.KeyNumber[0]), nil
			}
			logrus.Infof("[AddPubkey]User '%s' add pubkey successful,ID-KeyNumber:%d/%d\n",
				username, c.UserId, resp.KeyNumber[0])
		}
		logrus.Errorf("[AddPubkey]Return err msg.\n")
		return 0, errors.New("return err msg")
	}
}

type DownloadForSGX struct {
	DownloadObject
	Refs map[int32]*pkt.Refer
}

func (down *DownloadForSGX) GetRefers() {
	refmap := make(map[int32]*pkt.Refer)
	for _, ref := range down.REFS {
		id := int32(ref.Id) & 0xFFFF
		refmap[id] = ref
	}
	down.Refs = refmap
}

func (down *DownloadForSGX) LoadBlock(id int32) ([]byte, *pkt.ErrorMessage) {
	sgx, err := down.LoadEncryptedBlock(id)
	if err != nil {
		return nil, err
	} else {
		if sgx == nil {
			return nil, nil
		}
		return sgx.ToBytes(), nil
	}
}

func (down *DownloadForSGX) LoadEncryptedBlock(id int32) (*sgx.EncryptedBlock, *pkt.ErrorMessage) {
	refer := down.Refs[id]
	if refer == nil {
		return nil, nil
	}
	dn := &DownloadBlock{UClient: down.UClient, Ref: refer}
	eb, errmsg := dn.LoadEncryptedBlock()
	if errmsg != nil {
		return nil, errmsg
	}
	sgxb := &sgx.EncryptedBlock{}
	sgxb.DATA = eb.Data
	sgxb.KEU = refer.KEU
	sgxb.KeyNumber = int32(refer.KeyNumber)
	return sgxb, nil
}
