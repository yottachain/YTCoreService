package main

import (
	"fmt"
	"testing"

	"github.com/mr-tron/base58/base58"
	"github.com/yottachain/YTCoreService/examples"
	"github.com/yottachain/YTCrypto"
)

func Test_Test(t *testing.T) {
	s, _ := YTCrypto.Sign("5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH", []byte("lsptest"))

	fmt.Println(s)
	pub, _ := YTCrypto.GetPublicKeyByPrivateKey("5JnLRW1bTRD2bxo93wZ1qnpXfMDHzA97qcQjabnoqgmJTt7kBoH")

	bs, _ := YTCrypto.ECCEncrypt([]byte("lsptest"), pub)
	fmt.Println(base58.Encode(bs))
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

func Test_Codec(t *testing.T) {
	examples.TestCodec()
}
