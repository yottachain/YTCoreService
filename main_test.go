package main

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/yottachain/YTCoreService/examples"
	"github.com/yottachain/YTCoreService/pkt"
)

func Test_Test(t *testing.T) {

	ss := "YuNDoQEABH0AAAAAH/99H/9/kc39xQbYN6Mw2U8Kbo8yjEHEn99SQig+xaRxCDI1ZnoAAAAApA=="
	bs, _ := base64.StdEncoding.DecodeString(ss)

	refer := pkt.NewRefer(bs)
	id := int32(refer.Id) & 0xFFFF
	fmt.Println(int(id))
	fmt.Println(int(refer.Dup))
	fmt.Println(int(refer.RealSize))
	fmt.Println(uint64(refer.ShdCount))

	ss = "YuNDoQEABSEAAAAAHQmDHQmFrmhrCEnTho7zZ/5WC7eKqEQmssVUkB0jxu4uG8YjLksAAQAAmA=="
	bs, _ = base64.StdEncoding.DecodeString(ss)

	refer = pkt.NewRefer(bs)
	id = int32(refer.Id) & 0xFFFF
	fmt.Println(int(id))
	fmt.Println(int(refer.Dup))
	fmt.Println(int(refer.RealSize))
	fmt.Println(uint64(refer.ShdCount))
}

func Test_S3(t *testing.T) {
	examples.S3()
}

func Test_MakeConst(t *testing.T) {
	Make()
}

func Test_SetVersion(t *testing.T) {
	SetVersion()
}
