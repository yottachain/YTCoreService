package test

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/yottachain/YTCoreService/pkt"
	"google.golang.org/protobuf/proto"
)

func TestPkt() {
	nameTag := 0x4a
	field_number := nameTag >> 3
	wire_type := nameTag & 3
	fmt.Printf("%d-------------%d\n", field_number, wire_type)

	m := &pkt.StringMap{}
	m.Keys = []string{"aaa", "ccc"}
	m.Vals = make([]*pkt.StringMap_Vals, 2)
	s1 := "bbb"
	s2 := "ddd"
	m.Vals[0] = &pkt.StringMap_Vals{Val: &s1}
	m.Vals[1] = &pkt.StringMap_Vals{Val: &s2}

	res, _ := proto.Marshal(m)
	fmt.Println("bs:", hex.EncodeToString(res))

	//ss:=java object data
	ss := "0a036161610a03636363134a0362626214134a0364646414"
	fmt.Println("bs:", ss)

	bs, _ := hex.DecodeString(ss)
	msg := &pkt.StringMap{}

	if bytes.Equal(res, bs) {
		fmt.Printf("OK\n")
	}
	err := proto.Unmarshal(bs, msg)
	if err != nil {
		return
	}
	fmt.Printf("Map:%d\n", len(msg.Keys))
}
