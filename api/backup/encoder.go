package api

import (
	"bytes"
	"io"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/mr-tron/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/net"
	"github.com/yottachain/YTCoreService/pkt"
)

type Encoder struct {
	in  string
	key string
	fc  *codec.FileEncoder
}

func NewEncoder(inpath, s3key string) (*Encoder, error) {
	en := &Encoder{in: inpath, key: s3key}
	enc, err := codec.NewFileEncoder(en.in)
	if err != nil {
		return nil, err
	}
	en.fc = enc
	return en, nil
}

func (self *Encoder) GetSHA256() []byte {
	return self.fc.GetVHW()
}

func (self *Encoder) GetBaseSHA256() string {
	return base58.Encode(self.fc.GetVHW())
}

func (self *Encoder) GetMD5() []byte {
	return self.fc.GetMD5()
}

func (self *Encoder) GetBaseMD5() string {
	return base58.Encode(self.fc.GetMD5())
}

func (self *Encoder) Close() {
	if self.fc != nil {
		self.fc.Close()
		self.fc = nil
	}
}

func (self *Encoder) Handle(out string) error {
	defer self.Close()
	f, err := os.OpenFile(out+self.GetBaseSHA256(), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	size, err := self.writeHead(f)
	if err != nil {
		return err
	}
	var lastpos int64 = size
	for {
		b, err := self.fc.ReadNext()
		if err != nil {
			return err
		}
		if b == nil {
			break
		} else {
			size, err = self.writeEncryptedBlock(f, b)
			if err != nil {
				return err
			}
			lastpos = lastpos + size
		}
	}
	err = self.writeKey(f)
	if err != nil {
		return err
	}
	err = self.writeBottonPos(f, lastpos)
	if err != nil {
		return err
	}
	return nil
}

func (self *Encoder) writeEncryptedBlock(f *os.File, b *codec.PlainBlock) (int64, error) {
	b.Sum()
	SN := net.GetBlockSuperNode(b.VHP)
	req := &pkt.CheckBlockDupReq{
		UserId:    &BackupClient.UserId,
		SignData:  &BackupClient.Sign,
		KeyNumber: &BackupClient.KeyNumber,
		VHP:       b.VHP,
	}
	var resp proto.Message
	for {
		res, errmsg := net.RequestSN(req, SN, "", env.SN_RETRYTIMES, false)
		if errmsg != nil {
			logrus.Warnf("[Encode]%s,CheckBlockDup ERR:%s\n", self.in, pkt.ToError(errmsg))
			time.Sleep(time.Duration(env.SN_RETRY_WAIT) * time.Second)
		} else {
			resp = res
			break
		}
	}
	dupResp, ok := resp.(*pkt.UploadBlockDupResp)
	if ok {
		keu, vhb := self.CheckBlockDup(dupResp, b)
		if keu == nil {
			return self.writeNoDupBlock(f, b)
		} else {
			return self.writeDupBlock(f, b, keu, vhb)
		}
	} else {
		return self.writeNoDupBlock(f, b)
	}
}

func (self *Encoder) writeNoDupBlock(f *os.File, b *codec.PlainBlock) (int64, error) {
	bs1 := []byte{0x00}
	bs2 := env.IdToBytes(b.OriginalSize)
	bs3 := env.IdToBytes(int64(len(b.Data)))
	ks := codec.GenerateRandomKey()
	aes := codec.NewBlockAESEncryptor(b, ks)
	eblk, err := aes.Encrypt()
	if err != nil {
		return 0, err
	}
	size := int64(len(eblk.Data))
	bs4 := env.IdToBytes(size)
	keu := codec.ECBEncryptNoPad(ks, BackupClient.AESKey)
	ked := codec.ECBEncryptNoPad(ks, b.KD)
	bss := bytes.Join([][]byte{bs1, bs2, bs3, bs4, b.VHP, keu, ked, eblk.Data}, []byte{})
	_, err = f.Write(bss)
	if err != nil {
		return 0, err
	}
	return 1 + 8 + 8 + 8 + 32 + 32 + 32 + size, nil
}

func (self *Encoder) writeDupBlock(f *os.File, b *codec.PlainBlock, keu, vhb []byte) (int64, error) {
	bs1 := []byte{0x01}
	bs2 := env.IdToBytes(b.OriginalSize)
	bs3 := env.IdToBytes(int64(len(b.Data)))
	bss := bytes.Join([][]byte{bs1, bs2, bs3, b.VHP, keu, vhb}, []byte{})
	_, err := f.Write(bss)
	if err != nil {
		return 0, err
	}
	return 1 + 8 + 8 + 32 + 32 + 16, nil
}

func (self *Encoder) CheckBlockDup(resp *pkt.UploadBlockDupResp, b *codec.PlainBlock) ([]byte, []byte) {
	keds := resp.Keds.KED
	vhbs := resp.Vhbs.VHB
	ars := resp.Ars.AR
	for index, ked := range keds {
		ks := codec.ECBDecryptNoPad(ked, b.KD)
		aes := codec.NewBlockAESEncryptor(b, ks)
		eblk, err := aes.Encrypt()
		if err != nil {
			logrus.Warnf("[Encode]%s,CheckBlockDup ERR:%s\n", self.in, err)
			return nil, nil
		}
		var vhb []byte
		if eblk.NeedEncode() {
			if ars[index] == codec.AR_RS_MODE {
				logrus.Warnf("[Encode]%s,CheckBlockDup ERR:RS Not supported\n", self.in)
				return nil, nil
			} else {
				enc := codec.NewErasureEncoder(eblk)
				err = enc.Encode()
				if err != nil {
					logrus.Warnf("[Encode]%s,CheckBlockDup ERR:%s\n", self.in, err)
					return nil, nil
				}
				vhb = eblk.VHB
			}
		} else {
			err = eblk.MakeVHB()
			if err != nil {
				logrus.Warnf("[Encode]%s,CheckBlockDup ERR:%s\n", self.in, err)
				return nil, nil
			}
			vhb = eblk.VHB
		}
		if bytes.Equal(vhb, vhbs[index]) {
			return codec.ECBEncryptNoPad(ks, BackupClient.AESKey), vhb
		}
	}
	return nil, nil
}

func (self *Encoder) writeHead(f *os.File) (int64, error) {
	bs1 := env.IdToBytes(0)
	bs2 := env.IdToBytes(self.fc.GetLength())
	bs3 := self.GetMD5()
	bs4 := env.Int32ToBytes(int32(BackupClient.UserId))
	bs5 := env.Int32ToBytes(int32(BackupClient.KeyNumber))
	bs7 := []byte(BackupClient.Sign)
	size := len(bs7)
	bs6 := env.Int32ToBytes(int32(size))
	bss := bytes.Join([][]byte{bs1, bs2, bs3, bs4, bs5, bs6, bs7}, []byte{})
	_, err := f.Write(bss)
	if err != nil {
		return 0, err
	}
	return 8 + 8 + 16 + 4 + 4 + 4 + int64(size), nil
}

func (self *Encoder) writeBottonPos(f *os.File, pos int64) error {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	bs := env.IdToBytes(pos)
	_, err = f.Write(bs)
	if err != nil {
		return err
	}
	return nil
}

func (self *Encoder) writeKey(f *os.File) error {
	return WriteKey(self.key, f)
}

func WriteKey(key string, f *os.File) error {
	bs := []byte(key)
	size := int32(len(bs))
	bs1 := env.Int32ToBytes(size)
	bss := bytes.Join([][]byte{bs1, bs}, []byte{})
	_, err := f.Write(bss)
	if err != nil {
		return err
	}
	return nil
}
