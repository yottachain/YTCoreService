package codec

import (
	"bytes"
	"crypto/md5"

	"github.com/yottachain/YTCoreService/env"
)

type LRCEncoder struct {
	EncBlock  *EncryptedBlock
	Shards    []*Shard
	DataCount int32
}

func NewLRCEncoder(block *EncryptedBlock) *LRCEncoder {
	me := &LRCEncoder{}
	me.EncBlock = block
	return me
}

func (me *LRCEncoder) MakeVHBCopyMode() {
	md5Digest := md5.New()
	md5Digest.Write(me.Shards[0].VHF)
	me.EncBlock.VHB = md5Digest.Sum(nil)
}

func (me *LRCEncoder) MakeVHBLRCMode() {
	md5Digest := md5.New()
	for _, s := range me.Shards {
		md5Digest.Write(s.VHF)
	}
	me.EncBlock.VHB = md5Digest.Sum(nil)
}

func (me *LRCEncoder) IsCopyShard() bool {
	return me.Shards[0].IsCopyShard()
}

func (me *LRCEncoder) Encode() error {
	if !me.EncBlock.NeedEncode() {
		err := me.EncBlock.MakeVHB()
		if err != nil {
			return err
		}
	}
	size := me.EncBlock.Length()
	shardsize := int64(env.PFL - 1)
	shardCount := size / shardsize
	remainSize := size % shardsize
	if shardCount == 0 {
		rlen := shardsize - size
		var sdata []byte
		fbs := []byte{0xFF}
		if rlen > 0 {
			sdata = bytes.Join([][]byte{fbs, me.EncBlock.Data, make([]byte, rlen)}, []byte{})
		} else {
			sdata = bytes.Join([][]byte{fbs, me.EncBlock.Data}, []byte{})
		}
		shard := &Shard{Data: sdata}
		shard.SumVHF()
		me.Shards = make([]*Shard, env.Default_PND)
		for ii := 0; ii < env.Default_PND; ii++ {
			me.Shards[ii] = shard
		}
		me.MakeVHBCopyMode()
		me.DataCount = 1
	} else {
		shards := [][]byte{}
		for ii := 0; ii < int(shardCount); ii++ {
			fbs := []byte{uint8(ii)}
			spos := int64(ii) * shardsize
			epos := int64(ii+1) * shardsize
			sdata := bytes.Join([][]byte{fbs, me.EncBlock.Data[spos:epos]}, []byte{})
			shards = append(shards, sdata)
		}
		if remainSize > 0 {
			fbs := []byte{uint8(shardCount)}
			rlen := shardsize - remainSize
			spos := shardCount * shardsize
			sdata := bytes.Join([][]byte{fbs, me.EncBlock.Data[spos:], make([]byte, rlen)}, []byte{})
			shards = append(shards, sdata)
		}
		me.DataCount = int32(len(shards))
		pdata, err := LRC_Encode(shards)
		if err != nil {
			return err
		}
		shards = append(shards, pdata...)
		count := len(shards)
		me.Shards = make([]*Shard, count)
		for index, bs := range shards {
			shard := &Shard{Data: bs}
			shard.SumVHF()
			me.Shards[index] = shard
		}
		me.MakeVHBLRCMode()

	}
	return nil
}
