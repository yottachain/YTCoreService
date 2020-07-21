package test

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
)

func TestLRC() {
	codec.InitLRC()
	bs, _ := ioutil.ReadFile("d://test.dat")

	b := new(codec.EncryptedBlock)
	b.Data = bs
	b.MakeVHB()
	fmt.Printf("HASH:%s\n", base58.Encode(b.VHB))
	size := b.Length()
	encoder := codec.NewLRCEncoder(b)
	encoder.Encode()
	shards := encoder.Shards

	decoder, _ := codec.NewLRCDecoder(size)
	var ok bool
	ok, _ = decoder.AddShard(shards[0].Data)
	ok, _ = decoder.AddShard(shards[3].Data)
	ok, _ = decoder.AddShard(shards[5].Data)
	ok, _ = decoder.AddShard(shards[6].Data)
	ok, _ = decoder.AddShard(shards[7].Data)
	ok, _ = decoder.AddShard(shards[8].Data)
	ok, _ = decoder.AddShard(shards[9].Data)
	ok, _ = decoder.AddShard(shards[10].Data)
	ok, _ = decoder.AddShard(shards[11].Data)
	ok, _ = decoder.AddShard(shards[12].Data)
	ok, _ = decoder.AddShard(shards[13].Data)
	ok, _ = decoder.AddShard(shards[14].Data)
	ok, _ = decoder.AddShard(shards[15].Data)
	ok, _ = decoder.AddShard(shards[16].Data)
	ok, _ = decoder.AddShard(shards[17].Data)
	ok, _ = decoder.AddShard(shards[18].Data)
	if ok {
		b = decoder.GetEncryptedBlock()
		b.MakeVHB()
		fmt.Printf("HASH:%s\n", base58.Encode(b.VHB))
		ioutil.WriteFile("d://test.0.dat", b.Data, 0777)
	}

}

var filepath = "D:/yts3_linux_1.0.0.14.tar.gz"

func CreateFileEncoder(readinmemory bool) *codec.FileEncoder {
	if !readinmemory {
		enc, err := codec.NewFileEncoder(filepath)
		if err != nil {
			panic(err)
		}
		return enc
	} else {
		f, err := ioutil.ReadFile(filepath)
		if err != nil {
			panic(err)
		}
		enc, err := codec.NewBytesEncoder(f)
		if err != nil {
			panic(err)
		}
		return enc
	}
}

func TestCodec() {
	key := codec.GenerateRandomKey()
	logrus.Infof("key:%s\n", hex.EncodeToString(key))

	enc := CreateFileEncoder(false)
	logrus.Infof("Hash:%s\n", hex.EncodeToString(enc.GetVHW()))

	dec := codec.NewFileDecoder(filepath + ".new")
	I := 0
	for {
		has, err := enc.HasNext()
		if err != nil {
			panic(err)
		}
		if has {
			block := enc.Next()
			aes := codec.NewBlockAESEncryptor(*block, key)
			eb, err := aes.Encrypt()
			if err != nil {
				panic(err)
			}
			eb.Save(filepath + strconv.Itoa(I))
			eb.Clear()
			logrus.Infof("Save %s%d ok\n", filepath, I)
			dec.AddEncryptedBlock(*eb)
			I++
		} else {
			break
		}
	}
	err := dec.Handle()
	if err != nil {
		panic(err)
	}
	logrus.Infof("Hash:%s\n", hex.EncodeToString(dec.GetVHW()))
}
