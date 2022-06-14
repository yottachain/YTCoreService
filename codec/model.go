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

const AR_DB_MODE = 0
const AR_COPY_MODE = -2
const AR_RS_MODE = -1

type Shard struct {
	Data []byte
	VHF  []byte
}

func (self *Shard) SumVHF() {
	md5Digest := md5.New()
	md5Digest.Write(self.Data)
	self.VHF = md5Digest.Sum(nil)
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

func (self *Block) Length() int64 {
	return int64(len(self.Data))
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
		return errors.New("")
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
		return errors.New("plainBlock sum:data is null")
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

func (self *PlainBlock) InMemory() bool {
	var encryptedBlockSize int
	bsize := len(self.Data)
	remain := bsize % 16
	if remain == 0 {
		encryptedBlockSize = bsize + 16
	} else {
		encryptedBlockSize = bsize + (16 - remain)
	}
	return encryptedBlockSize < env.PL2
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

func GetEncryptedBlockSize(orgsize int64) int64 {
	remain := orgsize % 16
	if remain == 0 {
		return orgsize + 16
	} else {
		return orgsize + (16 - remain)
	}
}

type EncryptedBlock struct {
	Block
	VHB       []byte
	SecretKey []byte
}

func (self *EncryptedBlock) MakeVHB() error {
	if self.Data == nil {
		return errors.New("encryptedBlock sum:data is null")
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

func (self *EncryptedBlock) NeedLRCEncode() bool {
	size := len(self.Data)
	return NeedLRCEncode(size)
}

func (self *EncryptedBlock) NeedEncode() bool {
	size := len(self.Data)
	return size >= env.PL2
}

func NeedLRCEncode(size int) bool {
	if size >= env.PL2 {
		shardsize := env.PFL - 1
		dataShardCount := size / shardsize
		if dataShardCount > 0 {
			return true
		}
	}
	return false
}
