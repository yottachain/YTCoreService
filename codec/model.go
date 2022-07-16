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

func (shd *Shard) SumVHF() {
	md5Digest := md5.New()
	md5Digest.Write(shd.Data)
	shd.VHF = md5Digest.Sum(nil)
}

func (shd *Shard) GetShardIndex() uint8 {
	return uint8(shd.Data[0])
}

func (shd *Shard) IsCopyShard() bool {
	return shd.Data[0] == 0xFF
}

func (shd *Shard) Clear() {
	shd.Data = nil
}

type Block struct {
	Data []byte
	Path string
}

func (blk *Block) Length() int64 {
	return int64(len(blk.Data))
}

func (blk *Block) Clear() {
	blk.Data = nil
}

func (blk *Block) Load() error {
	if blk.Data == nil {
		f, err := os.Open(blk.Path)
		if err != nil {
			return err
		}
		defer f.Close()
		data, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}
		blk.Data = data
	}
	return nil
}

func (blk *Block) Save(pathstr string) error {
	if blk.Data == nil {
		return errors.New("")
	}
	err := ioutil.WriteFile(pathstr, blk.Data, 0644)
	if err == nil {
		blk.Path = pathstr
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

func (pb *PlainBlock) Sum() error {
	if pb.Data == nil {
		return errors.New("plainBlock sum:data is null")
	}
	md5Digest := md5.New()
	md5Digest.Write(pb.Data)
	md5hash := md5Digest.Sum(nil)
	sha256Digest := sha256.New()
	sha256Digest.Write(pb.Data)
	pb.VHP = sha256Digest.Sum(nil)
	sha256Digest = sha256.New()
	_, err := sha256Digest.Write(pb.Data)
	if err != nil {
		return err
	}
	sha256Digest.Write(md5hash)
	pb.KD = sha256Digest.Sum(nil)
	return nil
}

func (pb *PlainBlock) InMemory() bool {
	var encryptedBlockSize int
	bsize := len(pb.Data)
	remain := bsize % 16
	if remain == 0 {
		encryptedBlockSize = bsize + 16
	} else {
		encryptedBlockSize = bsize + (16 - remain)
	}
	return encryptedBlockSize < env.PL2
}

func (pb *PlainBlock) ToJson() string {
	hash := make(map[string]string)
	hash["Path"] = pb.Path
	hash["OriginalSize"] = strconv.FormatInt(int64(pb.OriginalSize), 10)
	b, _ := json.Marshal(hash)
	return string(b)
}

func (pb *PlainBlock) GetEncryptedBlockSize() int64 {
	if pb.Data == nil {
		return 0
	}
	size := len(pb.Data)
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

func (eb *EncryptedBlock) MakeVHB() error {
	if eb.Data == nil {
		return errors.New("encryptedBlock sum:data is null")
	}
	sha256Digest := sha256.New()
	sha256Digest.Write(eb.Data)
	bs := sha256Digest.Sum(nil)
	md5Digest := md5.New()
	md5Digest.Write(eb.Data)
	md5Digest.Write(bs)
	eb.VHB = md5Digest.Sum(nil)
	return nil
}

func (eb *EncryptedBlock) NeedLRCEncode() bool {
	size := len(eb.Data)
	return NeedLRCEncode(size)
}

func (eb *EncryptedBlock) NeedEncode() bool {
	size := len(eb.Data)
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
