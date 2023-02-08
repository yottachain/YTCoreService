package main

import (
	"testing"

	"github.com/yottachain/YTCoreService/examples"
)

func Test_Test(t *testing.T) {

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
