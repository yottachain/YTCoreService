package api

import (
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/yottachain/YTCoreService/pkt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var Object_Info_CACHE = cache.New(time.Duration(60)*time.Second, time.Duration(60)*time.Second)

type ObjectInfo struct {
	Length     int64
	UClient    *Client
	REFS       map[int32]*BlockInfo
	ADDR       map[int32][]string
	ShardCount int32
}

type BlockInfo struct {
	VBI    int64
	VHF    [][]byte
	NodeID []int32
}

func NewObjectMeta(c *Client, bucketName, filename string, version primitive.ObjectID) (*ObjectInfo, *pkt.ErrorMessage) {
	key := fmt.Sprintf("%d/%s/%s/%s", c.UserId, bucketName, filename, version.Hex())
	v, found := Object_Info_CACHE.Get(key)
	if found {
		return v.(*ObjectInfo), nil
	}
	do := &DownloadObject{UClient: c}
	err := do.InitByKey(bucketName, filename, version)
	if err != nil {
		return nil, err
	}
	meta := &ObjectInfo{Length: do.Length, UClient: c, ADDR: make(map[int32][]string)}
	refmap := make(map[int32]*BlockInfo)
	count := 0
	for _, ref := range do.REFS {
		id := int32(ref.Id) & 0xFFFF
		b, err := meta.GetBlockInfo(ref)
		if err != nil {
			return nil, err
		}
		b.VBI = ref.VBI
		refmap[id] = b
		count = count + len(b.VHF)
	}
	meta.REFS = refmap
	meta.ShardCount = int32(count)
	Object_Info_CACHE.SetDefault(key, meta)
	return meta, nil
}

func (self *ObjectInfo) GetBlockInfo(refer *pkt.Refer) (*BlockInfo, *pkt.ErrorMessage) {
	b := &DownloadBlock{UClient: self.UClient, Ref: refer}
	resp, err := b.LoadMeta()
	if err != nil {
		return nil, err
	}
	_, OK := resp.(*pkt.DownloadBlockDBResp)
	if OK {
		return &BlockInfo{VHF: [][]byte{[]byte("")}, NodeID: []int32{0}}, nil
	} else {
		initresp, _ := resp.(*pkt.DownloadBlockInitResp)
		vhfs := initresp.Vhfs.VHF
		ids := initresp.Nids.Nodeids
		b := &BlockInfo{VHF: vhfs, NodeID: ids}
		for _, n := range initresp.Nlist.Ns {
			if n.Id != nil {
				self.ADDR[*n.Id] = n.Addrs
			}
		}
		return b, nil
	}
}
