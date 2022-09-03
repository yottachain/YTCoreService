package examples

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
)

const FileSize int64 = 1024*1024*9 + 8192

func TestCodec() {
	env.InitClient()
	codec.InitLRC()
	for ii := 0; ii < 20; ii++ {
		go func() {
			for ii := 0; ii < 3000; ii++ {
				codefile()
			}
			logrus.Infof("complete %d\n", ii)
		}()
	}
	select {}
}

func codefile() {
	var data []byte = env.MakeRandData(FileSize)
	enc, err := codec.NewBytesEncoder(data)
	if err != nil {
		panic(err)
	}
	b, err := enc.ReadNext()
	if err != nil {
		panic(err)
	}
	codeblock(b)
}

func codeblock(b *codec.PlainBlock) {
	er := b.Sum()
	if er != nil {
		panic(er)
	}
	size := len(b.Data)
	ks := codec.GenerateRandomKey()
	aes := codec.NewBlockAESEncryptor(b, ks)
	eblk, err := aes.Encrypt()
	if err != nil {
		panic(err)
	}
	enc := codec.NewErasureEncoder(eblk)
	err = enc.Encode()
	if err != nil {
		panic(err)
	}

	rsize := codec.GetEncryptedBlockSize(int64(size))
	c, err := codec.NewErasureDecoder(rsize)
	if err != nil {
		panic(err)
	}

	shds := enc.Shards
	rand.Seed(time.Now().Unix())
	rand.Shuffle(len(shds), func(i, j int) { shds[i], shds[j] = shds[j], shds[i] })
	for _, shd := range shds {
		b, err := c.AddShard(shd.Data)
		if err != nil {
			panic(err)
		}
		if b {
			break
		}
	}
	if !c.IsOK() {
		panic(errors.New("shards is not enough"))
	}
	newblk := c.GetEncryptedBlock()
	newblk.SecretKey = ks
	dec := codec.NewBlockAESDecryptor(newblk)
	pb, err := dec.Decrypt()
	if err != nil {
		panic(err)
	}
	er = pb.Sum()
	if er != nil {
		panic(er)
	}
	if bytes.Equal(b.VHP, pb.VHP) {
		fmt.Println("ok")
	}
}
