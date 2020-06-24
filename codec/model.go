package codec

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/yottachain/YTCoreService/env"
)

type Shard struct {
	Data []byte
	VHF  []byte
}

func (self *Shard) GetShardIndex() uint8 {
	return uint8(self.Data[0])
}

func (self *Shard) IsCopyShard() bool {
	return self.Data[0] == 0xFF
}

func (self *Shard) Clear() {
	self.Data = nil
}

type Block struct {
	Data []byte
	Path string
}

func (self *Block) Clear() {
	self.Data = nil
}

func (self *Block) Load() error {
	if self.Data == nil {
		f, err := os.Open(self.Path)
		if err != nil {
			return err
		}
		defer f.Close()
		data, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}
		self.Data = data
	}
	return nil
}

func (self *Block) Save(pathstr string) error {
	if self.Data == nil {
		return errors.New("data is null")
	}
	err := ioutil.WriteFile(pathstr, self.Data, 0644)
	if err == nil {
		self.Path = pathstr
	}
	return err
}

type PlainBlock struct {
	Block
	OriginalSize int64
	VHP          []byte
	KD           []byte
}

func NewPlainBlock(bs []byte, size int64) *PlainBlock {
	b := new(PlainBlock)
	b.Data = bs
	b.OriginalSize = size
	return b
}

func InitPlainBlock(jsonstr string) (*PlainBlock, error) {
	b := new(PlainBlock)
	hash := map[string]string{}
	err := json.Unmarshal([]byte(jsonstr), &hash)
	if err == nil {
		b.Path = hash["Path"]
		ii, _ := strconv.ParseInt(hash["OriginalSize"], 10, 64)
		b.OriginalSize = int64(ii)
		return b, nil
	}
	return nil, err
}

func (self *PlainBlock) Sum() error {
	if self.Data == nil {
		return errors.New("data is null")
	}
	md5Digest := md5.New()
	md5Digest.Write(self.Data)
	md5hash := md5Digest.Sum(nil)
	sha256Digest := sha256.New()
	sha256Digest.Write(self.Data)
	self.VHP = sha256Digest.Sum(nil)
	sha256Digest = sha256.New()
	_, err := sha256Digest.Write(self.Data)
	if err != nil {
		return err
	}
	sha256Digest.Write(md5hash)
	self.KD = sha256Digest.Sum(nil)
	return nil
}

func (self *PlainBlock) ToJson() string {
	hash := make(map[string]string)
	hash["Path"] = self.Path
	hash["OriginalSize"] = strconv.FormatInt(int64(self.OriginalSize), 10)
	b, _ := json.Marshal(hash)
	return string(b)
}

func (self *PlainBlock) GetEncryptedBlockSize() int64 {
	if self.Data == nil {
		return 0
	}
	size := len(self.Data)
	remain := size % 16
	if remain == 0 {
		return int64(size + 16)
	} else {
		return int64(size + (16 - remain))
	}
}

type EncryptedBlock struct {
	Block
	VHB       []byte
	SecretKey []byte
}

func (self *EncryptedBlock) MakeVHB() error {
	if self.Data == nil {
		return errors.New("data is null")
	}
	sha256Digest := sha256.New()
	sha256Digest.Write(self.Data)
	bs := sha256Digest.Sum(nil)
	md5Digest := md5.New()
	md5Digest.Write(self.Data)
	md5Digest.Write(bs)
	self.VHB = md5Digest.Sum(nil)
	return nil
}

func NeedLRCEncode(size int32) bool {
	if size >= env.PL2 {
		shardsize := env.PFL - 1
		dataShardCount := size / int32(shardsize)
		if dataShardCount > 0 {
			return true
		}
	}
	return false
}

func NeedEncode(size int32) bool {
	return size >= env.PL2
}
