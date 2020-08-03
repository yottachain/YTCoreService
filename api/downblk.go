package api

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

type DownloadBlock struct {
	UClient *Client
	Ref     *pkt.Refer
	BLK     *codec.PlainBlock
}

func (self DownloadBlock) load() *pkt.ErrorMessage {
	KS := codec.ECBDecryptNoPad(self.Ref.KEU, self.UClient.AESKey)
	vbi := uint64(self.Ref.VBI)
	req := &pkt.DownloadBlockInitReqV2{
		UserId:    &self.UClient.UserId,
		SignData:  &self.UClient.Sign,
		KeyNumber: &self.UClient.KeyNumber,
		VBI:       &vbi,
	}
	startTime := time.Now()
	sn := net.GetSuperNode(int(self.Ref.SuperID))
	if sn == nil {
		logrus.Errorf("[DownloadBlock][%d][%d]Init ERR:SNID %d\n", self.Ref.Id, self.Ref.VBI, self.Ref.SuperID)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, "SNID_ERR")
	}
	resp, errmsg := net.RequestSN(req, sn, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[DownloadBlock][%d][%d]Init ERR:%s\n", self.Ref.Id, self.Ref.VBI, pkt.ToError(errmsg))
		return errmsg
	} else {
		dbresp, OK := resp.(*pkt.DownloadBlockDBResp)
		if OK {
			b := &codec.EncryptedBlock{SecretKey: KS}
			b.Data = dbresp.Data
			errmsg = self.aesDecode(b)
			if errmsg != nil {
				return errmsg
			} else {
				logrus.Infof("[DownloadBlock][%d][%d]Download Block from DB,at sn %d, take times %d ms.\n",
					self.Ref.Id, self.Ref.VBI, self.Ref.SuperID, time.Now().Sub(startTime).Milliseconds())
				return nil
			}
		}
		initresp, OK := resp.(*pkt.DownloadBlockInitResp)
		if OK {
			if initresp.Vhfs == nil || initresp.Vhfs.VHF == nil || len(initresp.Vhfs.VHF) == 0 {
				if initresp.Nids == nil || initresp.Nids.Nodeids == nil || len(initresp.Nids.Nodeids) == 0 {
					if len(initresp.Vhfs.VHF) != len(initresp.Nids.Nodeids) {
						logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:RETURN_ERR_VHF\n", self.Ref.Id, self.Ref.VBI)
						return pkt.NewErrorMsg(pkt.SERVER_ERROR, "VHF_ERR")
					}
				}
			}
			if initresp.Nlist == nil || initresp.Nlist.Ns == nil || len(initresp.Nlist.Ns) == 0 {
				logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:RETURN_ERR_NODELIST\n", self.Ref.Id, self.Ref.VBI)
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Node_List_ERR")
			}
			logrus.Infof("[DownloadBlock][%d][%d]Init OK,at sn %d,take times %d ms.\n",
				self.Ref.Id, self.Ref.VBI, self.Ref.SuperID, time.Now().Sub(startTime).Milliseconds())
			m := initresp.GetAR()
			if m == codec.AR_COPY_MODE {
				return self.loadCopyShard(KS, initresp)
			}
			if m > 0 {
				return self.loadLRCShard(initresp)
			}
			logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:Not supported,AR:%d.\n", self.Ref.Id, self.Ref.VBI, m)
			return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Not supported")
		}
		logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:RETURN_ERR_MSG\n", self.Ref.Id, self.Ref.VBI)
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
}

func (self DownloadBlock) loadLRCShard(resp *pkt.DownloadBlockInitResp) *pkt.ErrorMessage {
	//for _, n := range resp.Nlist.Ns {

	//}

	return nil
}

func (self DownloadBlock) loadCopyShard(ks []byte, resp *pkt.DownloadBlockInitResp) *pkt.ErrorMessage {
	vhf := resp.Vhfs.VHF[0]
	s := fmt.Sprintf("[%d][%d]", self.Ref.Id, self.Ref.VBI)
	var b []byte
	for _, n := range resp.Nlist.Ns {
		dnshard := NewDownLoadShardInfo(n, vhf, s, 0)
		if dnshard != nil {
			dnshard.Download()
			if dnshard.Data != nil {
				b = dnshard.Data
				break
			}
		}
	}
	if b == nil {
		logrus.Errorf("[DownloadBlock][%d][%d]Download copymode shard ERR,count %d\n", self.Ref.Id, self.Ref.VBI, resp.GetVNF())
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, "COMM_ERROR")
	} else {
		eb := &codec.EncryptedBlock{SecretKey: ks}
		eb.Data = b[1:]
		return self.aesDecode(eb)
	}
}

func (self DownloadBlock) aesDecode(b *codec.EncryptedBlock) *pkt.ErrorMessage {
	dec := codec.NewBlockAESDecryptor(b)
	pb, err := dec.Decrypt()
	if err != nil {
		logrus.Errorf("[DownloadBlock][%d][%d]AESDecode ERR:%s\n", self.Ref.Id, self.Ref.VBI, err)
		return pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	self.BLK = pb
	return nil
}
