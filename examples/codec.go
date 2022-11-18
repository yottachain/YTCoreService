package examples

import (
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/env"
)

const FileSize int64 = 1024*1024*9 - 204

func TestCodec() {
	env.InitClient()
	codec.InitLRC()
	loop := 10
	filenum := 40
	stime := time.Now()
	wgroup := sync.WaitGroup{}
	for ii := 0; ii < loop; ii++ {
		id := ii
		wgroup.Add(1)
		go func() {
			for i := 0; i < filenum; i++ {
				codefile()
			}
			logrus.Infof("complete %d\n", id)
			wgroup.Done()
		}()
	}
	wgroup.Wait()
	size := loop * filenum * 9
	s := int(time.Since(stime).Seconds())
	logrus.Infof("平均速度：%dM/s\n", size/s)
	select {}
}

func codefile() {
	var data []byte = env.MakeRandData(FileSize)
	enc, err := codec.NewBytesEncoder(data)
	if err != nil {
		panic(err)
	}
	for {
		b, err := enc.ReadNext()
		if err != nil {
			panic(err)
		}
		if b == nil {
			break
		}
		codeblock(b)
	}

}

func codeblock(b *codec.PlainBlock) {
	er := b.Sum()
	if er != nil {
		panic(er)
	}
	//size := len(b.Data)
	ks := codec.GenerateRandomKey()
	aes := codec.NewBlockAESEncryptor(b, ks)
	_, err := aes.Encrypt()
	if err != nil {
		panic(err)
	}
	/*
		enc := codec.NewErasureEncoder(eblk)
		err = enc.Encode()
		if err != nil {
			panic(err)
		}

		rsize := codec.GetEncryptedBlockSize(int64(size))
		_, err = codec.NewErasureDecoder(rsize)
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
			}*/
	fmt.Println("ok")
}
