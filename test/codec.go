package test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
)

func Codec() {
	env.InitClient()
	//TestMultiCutFile()
	//TestCutFile()
	TestLRC()
}

func TestAES() {
	data := []byte("abcdefg")
	sha256Digest := sha256.New()
	sha256Digest.Write(data)
	newdata := sha256Digest.Sum(nil)
	fmt.Printf("data:%s\n", base58.Encode(newdata))
	key := []byte("123456789")
	sha256Digest = sha256.New()
	sha256Digest.Write(key)
	newkey := sha256Digest.Sum(nil)

	bs := codec.ECBEncryptNoPad(newdata, newkey)
	ss := base58.Encode(bs)
	fmt.Printf("Encode:%s\n", ss)

	d := codec.ECBDecryptNoPad(bs, newkey)
	fmt.Printf("data:%s\n", base58.Encode(d))
}

func TestLRCLoop() {
	env.InitClient()
	codec.InitLRC()
	myfunc := func() {
		for ii := 0; ii < 1000; ii++ {
			bs := env.MakeRandData(1024*1024*2 - 256)
			b := &codec.EncryptedBlock{}
			b.Data = bs
			encoder := codec.NewErasureEncoder(b)
			err := encoder.Encode()
			if err != nil {
				logrus.Panicf("Encode ERR:%s\n", err)
			}
			shards := encoder.Shards
			fmt.Printf("Encode OK:%d/%d\n", len(shards), ii)

			decoder, _ := codec.NewErasureDecoder(int64(len(bs)))
			count := 0
			for {
				ii := time.Now().UnixNano() % int64(len(shards))
				shard := shards[ii]
				ok, _ := decoder.AddShard(shard.Data)
				shards = append(shards[:ii], shards[ii+1:]...)
				if len(shards) == 0 {
					logrus.Panicf("Decode ERR:%d shards\n", len(encoder.Shards))
				}
				count++
				if ok {
					break
				}
			}
			fmt.Printf("Decode OK,input:%d\n", count)

		}
	}
	for ii := 0; ii < 30; ii++ {
		go myfunc()
	}
}

func TestLRC() {
	//execute only once when process starts
	codec.InitLRC()
	//input: <2M file
	bs, _ := ioutil.ReadFile("d://test.docx")
	b := &codec.EncryptedBlock{}
	b.Data = bs
	b.MakeVHB()
	fmt.Printf("HASH:%s\n", base58.Encode(b.VHB))
	//remember original size
	size := b.Length()

	encoder := codec.NewErasureEncoder(b)
	encoder.Encode()
	//encode result
	shards := encoder.Shards
	fmt.Printf("Encode OK:%d\n", len(shards))
	//decode:input original size
	decoder, _ := codec.NewErasureDecoder(size)

	//Random input shard
	count := 0
	for {
		ii := time.Now().UnixNano() % int64(len(shards))
		shard := shards[ii]
		ok, _ := decoder.AddShard(shard.Data)
		shards = append(shards[:ii], shards[ii+1:]...)
		count++
		fmt.Printf("Add index:%d,total:%d\n", uint16(shard.Data[0]), count)
		if ok {
			break
		}
	}
	fmt.Printf("Decode OK:input %d\n", count)
	//decode result
	b = decoder.GetEncryptedBlock()
	b.MakeVHB()
	fmt.Printf("HASH:%s\n", base58.Encode(b.VHB))
	ioutil.WriteFile("d://test.0.docx", b.Data, 0777)
}

var filepath = "D:/test/Windows2012.iso"

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

func TestMultiCutFile() {
	env.InitClient()
	key := codec.GenerateRandomKey()
	logrus.Infof("key:%s\n", hex.EncodeToString(key))
	//paths := []string{"d:/p2p/1_p2p-wrapper-0.1.jar", "d:/p2p/2_p2p-wrapper-0.1.jar",
	//	"d:/p2p/3_p2p-wrapper-0.1.jar", "d:/p2p/4_p2p-wrapper-0.1.jar", "d:/p2p/5_p2p-wrapper-0.1.jar",
	//	"d:/p2p/6_p2p-wrapper-0.1.jar"}
	paths := make([]string, 558)
	for ii := 1; ii <= 558; ii++ {
		paths[ii-1] = fmt.Sprintf("D:/Windows2012R2.iso/%d", ii)
	}

	enc, err1 := codec.NewMultiFileEncoder(paths)
	if err1 != nil {
		panic(err1)
	}
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
			logrus.Infof("b%d osize:%d,rsize:%d \n", I, block.OriginalSize, len(block.Data))
			aes := codec.NewBlockAESEncryptor(block, key)
			eb, err := aes.Encrypt()
			if err != nil {
				panic(err)
			}
			eb.Save(filepath + strconv.Itoa(I))
			eb.Clear()
			logrus.Infof("Save %s%d ok\n", filepath, I)
			dec.AddEncryptedBlock(eb)
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

func TestCutFile() {
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
			logrus.Infof("b%d osize:%d,rsize:%d \n", I, block.OriginalSize, len(block.Data))
			aes := codec.NewBlockAESEncryptor(block, key)
			eb, err := aes.Encrypt()
			if err != nil {
				panic(err)
			}
			eb.Save(filepath + strconv.Itoa(I))
			eb.Clear()
			logrus.Infof("Save %s%d ok\n", filepath, I)
			dec.AddEncryptedBlock(eb)
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
