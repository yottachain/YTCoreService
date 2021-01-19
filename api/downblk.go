package api

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

type DownloadBlock struct {
	UClient *Client
	Ref     *pkt.Refer
	Path    string
	KS      []byte
}

func (self DownloadBlock) LoadMeta() (proto.Message, *pkt.ErrorMessage) {
	vbi := uint64(self.Ref.VBI)
	req := &pkt.DownloadBlockInitReqV2{
		UserId:    &self.UClient.UserId,
		SignData:  &self.UClient.SignKey.Sign,
		KeyNumber: &self.UClient.SignKey.KeyNumber,
		VBI:       &vbi,
	}
	sn := net.GetSuperNode(int(self.Ref.SuperID))
	if sn == nil {
		logrus.Errorf("[DownloadBlock][%d][%d]Init ERR:SNID %d\n", self.Ref.Id, self.Ref.VBI, self.Ref.SuperID)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, "SNID_ERR")
	}
	resp, errmsg := net.RequestSN(req, sn, "", env.SN_RETRYTIMES, false)
	if errmsg != nil {
		logrus.Errorf("[DownloadBlock][%d][%d]Init ERR:%s\n", self.Ref.Id, self.Ref.VBI, pkt.ToError(errmsg))
		return nil, errmsg
	} else {
		dbresp, OK := resp.(*pkt.DownloadBlockDBResp)
		if OK {
			if dbresp.Data == nil || len(dbresp.Data) == 0 {
				logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:RETURN_NULL_DATA\n", self.Ref.Id, self.Ref.VBI)
				return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "DATA_ERR")
			}
			return dbresp, nil
		}
		initresp, OK := resp.(*pkt.DownloadBlockInitResp)
		if OK {
			if initresp.Vhfs == nil || initresp.Vhfs.VHF == nil || len(initresp.Vhfs.VHF) == 0 {
				if initresp.Nids == nil || initresp.Nids.Nodeids == nil || len(initresp.Nids.Nodeids) == 0 {
					if len(initresp.Vhfs.VHF) != len(initresp.Nids.Nodeids) {
						logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:RETURN_ERR_VHF\n", self.Ref.Id, self.Ref.VBI)
						return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "VHF_ERR")
					}
				}
			}
			if initresp.Nlist == nil || initresp.Nlist.Ns == nil || len(initresp.Nlist.Ns) == 0 {
				logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:RETURN_ERR_NODELIST\n", self.Ref.Id, self.Ref.VBI)
				return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Node_List_ERR")
			}
			return initresp, nil
		}
		logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:RETURN_ERR_MSG\n", self.Ref.Id, self.Ref.VBI)
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Return err msg type")
	}
}

func (self DownloadBlock) Load() (*codec.PlainBlock, *pkt.ErrorMessage) {
	KS := self.KS
	if KS == nil {
		k, ok := self.UClient.KeyMap[uint32(self.Ref.KeyNumber)]
		if !ok {
			emsg := fmt.Sprintf("The user did not enter a private key with number%d", self.Ref.KeyNumber)
			logrus.Errorf("[DownloadBlock]%s\n", emsg)
			return nil, pkt.NewErrorMsg(pkt.PRIKEY_NOT_EXIST, emsg)
		}
		KS = codec.ECBDecryptNoPad(self.Ref.KEU, k.AESKey)
	}
	startTime := time.Now()
	resp, errmsg := self.LoadMeta()
	if errmsg != nil {
		return nil, errmsg
	} else {
		dbresp, OK := resp.(*pkt.DownloadBlockDBResp)
		if OK {
			b := &codec.EncryptedBlock{SecretKey: KS}
			b.Data = dbresp.Data
			bp, errmsg := self.aesDecode(b)
			if errmsg != nil {
				return nil, errmsg
			} else {
				logrus.Infof("[DownloadBlock][%d][%d]Download Block from DB,at sn %d, take times %d ms.\n",
					self.Ref.Id, self.Ref.VBI, self.Ref.SuperID, time.Now().Sub(startTime).Milliseconds())
				return bp, nil
			}
		}
		initresp, _ := resp.(*pkt.DownloadBlockInitResp)
		logrus.Infof("[DownloadBlock][%d][%d]Init OK,at sn %d,take times %d ms.\n",
			self.Ref.Id, self.Ref.VBI, self.Ref.SuperID, time.Now().Sub(startTime).Milliseconds())
		startTime := time.Now()
		m := initresp.GetAR()
		if m == codec.AR_COPY_MODE {
			bp, errmsg := self.loadCopyShard(KS, initresp)
			if errmsg != nil {
				return nil, errmsg
			} else {
				logrus.Infof("[DownloadBlock][%d][%d]Download CopyMode Block OK, take times %d ms.\n",
					self.Ref.Id, self.Ref.VBI, time.Now().Sub(startTime).Milliseconds())
				return bp, nil
			}
		}
		if m > 0 {
			bp, errmsg := self.loadLRCShard(KS, initresp)
			if errmsg != nil {
				return nil, errmsg
			} else {
				logrus.Infof("[DownloadBlock][%d][%d]Download LRCMode Block OK, take times %d ms.\n",
					self.Ref.Id, self.Ref.VBI, time.Now().Sub(startTime).Milliseconds())
				return bp, nil
			}
		}
		logrus.Errorf("[DownloadBlock][%d][%d]Download init ERR:Not supported,AR:%d.\n", self.Ref.Id, self.Ref.VBI, m)
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "Not supported")
	}
}

func (self DownloadBlock) loadLRCShard(ks []byte, resp *pkt.DownloadBlockInitResp) (*codec.PlainBlock, *pkt.ErrorMessage) {
	dns := NewDownLoad(fmt.Sprintf("[%d][%d]", self.Ref.Id, self.Ref.VBI), 0)
	downloads := []*DownLoadShardInfo{}
	for ii, id := range resp.Nids.Nodeids {
		vhf := resp.Vhfs.VHF[ii]
		var dn *DownLoadShardInfo
		for _, n := range resp.Nlist.Ns {
			if n != nil {
				if n.Id != nil && id == *n.Id {
					dn = NewDownLoadShardInfo(n, vhf, env.DownloadRetryTimes, dns, self.Path)
					break
				}
			}
		}
		if dn != nil {
			downloads = append(downloads, dn)
		}
	}
	size := len(downloads)
	err := dns.CreateErasureDecoder(codec.GetEncryptedBlockSize(int64(self.Ref.RealSize)), size)
	if err != nil {
		logrus.Errorf("[DownloadBlock][%d][%d]CreateLRCDecoder ERR:%s\n", self.Ref.Id, self.Ref.VBI, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	logrus.Infof("[DownloadBlock][%d][%d]Start downloading shards,total %d\n", self.Ref.Id, self.Ref.VBI, size)
	for _, dn := range downloads {
		<-SHARD_DOWN_CH
		go dn.Download()
	}
	b, err1 := dns.WaitDownload(size)
	if err1 != nil {
		logrus.Errorf("[DownloadBlock][%d][%d]Download ERR:%s\n", self.Ref.Id, self.Ref.VBI, err1)
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, err1.Error())
	}
	b.SecretKey = ks
	return self.aesDecode(b)
}

func (self DownloadBlock) loadCopyShard(ks []byte, resp *pkt.DownloadBlockInitResp) (*codec.PlainBlock, *pkt.ErrorMessage) {
	vhf := resp.Vhfs.VHF[0]
	var b []byte
	dns := NewDownLoad(fmt.Sprintf("[%d][%d]", self.Ref.Id, self.Ref.VBI), len(resp.Nlist.Ns))
	for _, n := range resp.Nlist.Ns {
		if n != nil {
			dnshard := NewDownLoadShardInfo(n, vhf, 0, dns, self.Path)
			if dnshard != nil {
				<-SHARD_DOWN_CH
				b = dnshard.Download()
				if b != nil {
					break
				}
			}
		}
	}
	if b == nil {
		logrus.Errorf("[DownloadBlock][%d][%d]Download copymode shard ERR,count %d\n", self.Ref.Id, self.Ref.VBI, resp.GetVNF())
		return nil, pkt.NewErrorMsg(pkt.SERVER_ERROR, "COMM_ERROR")
	} else {
		c, _ := codec.NewErasureDecoder(codec.GetEncryptedBlockSize(int64(self.Ref.RealSize)))
		c.AddShard(b)
		eb := c.GetEncryptedBlock()
		eb.SecretKey = ks
		return self.aesDecode(eb)
	}
}

func (self DownloadBlock) aesDecode(b *codec.EncryptedBlock) (*codec.PlainBlock, *pkt.ErrorMessage) {
	if self.Path != "" {
		b.Save(self.Path + "block.enc")
	}
	dec := codec.NewBlockAESDecryptor(b)
	pb, err := dec.Decrypt()
	if err != nil {
		logrus.Errorf("[DownloadBlock][%d][%d]AESDecode ERR:%s\n", self.Ref.Id, self.Ref.VBI, err)
		return nil, pkt.NewErrorMsg(pkt.INVALID_ARGS, err.Error())
	}
	if self.Path != "" {
		pb.Save(self.Path + "block.zip")
	}
	return pb, nil
}
