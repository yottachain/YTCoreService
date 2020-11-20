package main

import (
	"fmt"
	"testing"

	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/test"
)

func init() {
	env.Console = true
}

type Human struct {
	name  string
	age   int
	phone string
}

type Student struct {
	Human  //匿名字段
	school string
}

func (h *Student) CallSayHi() {
	fmt.Printf("Student.SayHi\n")
}

type Employee struct {
	Human   //匿名字段
	company string
}

func (h *Employee) CallSayHi() {
	fmt.Printf("Employee.SayHi\n")
}

func (h *Human) SayHi() {
	h.CallSayHi()
}

func (h *Human) CallSayHi() {
	fmt.Printf("Human.SayHi\n")
}

func Test(t *testing.T) {
	defer env.TracePanic("Test")

	//***********api test*********
	//test.UpAndDownFile()
	//test.DownLoadByKey()
	//test.UpAndDownBytes()

	//************s3 test****************
	test.UploadFile()
	//test.ListBucket()
	//test.ListObj()

	//**********code test**********
	//env.InitClient()
	//test.TestMultiCutFile()
	//test.TestCutFile()
	//test.TestLRC()
	select {}
}
