package api

import (
	"errors"
	"fmt"

	"github.com/eoscanada/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
	"github.com/yottachain/YTCoreService/sgx"
)

func AddPublicKey(username, pubkey string) (uint32, error) {
	c := GetClientByName(username)
	if c == nil {
		return 0, fmt.Errorf("User '%s' not registered", username)
	}
	KUEp := base58.Decode(pubkey)
	if KUEp == nil || len(KUEp) != 37 {
		return 0, fmt.Errorf("Pubkey '%s' format error", pubkey)
	}
	cc := GetClient(pubkey)
	if cc != nil {
		for k, v := range cc.KeyMap {
			if v.PrivateKey == pubkey {
				return k, nil
			}
		}
	} else {
		return addPubkey(c, username, pubkey)
	}
	return 0, fmt.Errorf("User '%s' add pubkey failed", username)
}

func addPubkey(c *Client, username, pubkey string) (uint32, error) {
	sn := c.SuperNode
	req := &pkt.RegUserReqV2{Username: &username, PubKey: &pubkey, VersionId: &env.VersionID}
	res, err := net.RequestSN(req, sn, "", 1, false)
	if err != nil {
		emsg := fmt.Sprintf("User '%s' add pubkey failed!%s", username, pkt.ToError(err))
		logrus.Errorf("[AddPubkey]%s\n", emsg)
		return 0, errors.New(emsg)
	} else {
		resp, ok := res.(*pkt.RegUserResp)
		if ok {
			if resp.SuperNodeNum != nil && resp.UserId != nil && resp.KeyNumber != nil {
				if *resp.SuperNodeNum >= 0 && *resp.SuperNodeNum < uint32(net.GetSuperNodeCount()) {
					logrus.Infof("[AddPubkey]User '%s' add pubkey successful,ID-KeyNumber:%d/%d,at sn %d\n",
						username, c.UserId, *resp.KeyNumber, sn.ID)
					return *resp.KeyNumber, nil
				}
			}
		}
		logrus.Errorf("[AddPubkey]Return err msg.\n")
		return 0, errors.New("Return err msg")
	}
}

type DownloadForSGX struct {
	DownloadObject
	Refs map[int32]*pkt.Refer
}

func (self *DownloadForSGX) GetRefers() {
	refmap := make(map[int32]*pkt.Refer)
	for _, ref := range self.REFS {
		id := int32(ref.Id) & 0xFFFF
		refmap[id] = ref
	}
	self.Refs = refmap
}

func (self *DownloadForSGX) LoadBlock(id int32) ([]byte, *pkt.ErrorMessage) {
	sgx, err := self.LoadEncryptedBlock(id)
	if err != nil {
		return nil, err
	} else {
		if sgx == nil {
			return nil, nil
		}
		return sgx.ToBytes(), nil
	}
}

func (self *DownloadForSGX) LoadEncryptedBlock(id int32) (*sgx.EncryptedBlock, *pkt.ErrorMessage) {
	refer := self.Refs[id]
	if refer == nil {
		return nil, nil
	}
	dn := &DownloadBlock{UClient: self.UClient, Ref: refer}
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
