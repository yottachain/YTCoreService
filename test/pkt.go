package test

import (
	"encoding/hex"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/yottachain/YTCoreService/pkt"
)

func TestPkt() {
	snlist := make([]*pkt.ListSuperNodeResp_SuperNodes_SuperNode, 2)
	id := int32(1)
	nid := "sds"
	pkey := "NA"
	adds := []string{"s", "sd"}
	snlist[0] = &pkt.ListSuperNodeResp_SuperNodes_SuperNode{Id: &id, Nodeid: &nid, Pubkey: &pkey, Privkey: &pkey, Addrs: adds}
	snlist[1] = &pkt.ListSuperNodeResp_SuperNodes_SuperNode{Id: &id, Nodeid: &nid, Pubkey: &pkey, Privkey: &pkey, Addrs: adds}
	count := uint32(2)
	sns := &pkt.ListSuperNodeResp_SuperNodes{Count: &count, Supernode: snlist}
	resp := &pkt.ListSuperNodeResp{Supernodes: sns}
	//res, _ := proto.Marshal(resp)
	res, _ := proto.Marshal(resp)

	fmt.Println("bs:", hex.EncodeToString(res))

	/*
		ss := "bef5080312077364646464646418062204736473643308a2d398f60510efbffb0118cc48208be7be0134"
		fmt.Println("bs:", ss)
		bs, _ := hex.DecodeString(ss)
		msg := pkt.UnmarshalMsg(bs)
		cache, _ := msg.(*pkt.DownloadFileReqV2)
	*/
	//*cache.Node[0].Id = 3

	//id := ActiveCacheGetId(cache)
	//fmt.Println("req:", cache)

	//	bs1, _ := pkt.MarshalMsg(cache)

	//fmt.Println("bs:", hex.EncodeToString(bs1))
}
