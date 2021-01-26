package pkt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	proto "github.com/golang/protobuf/proto"
)

const SERVER_ERROR = 0x00
const INVALID_USER_ID = 0x01
const NOT_ENOUGH_DHH = 0x02
const COMM_ERROR = 0x03
const INVALID_UPLOAD_ID = 0x04
const TOO_MANY_SHARDS = 0x05
const ILLEGAL_VHP_NODEID = 0x06
const NO_SUCH_BLOCK = 0x07
const INVALID_VHB = 0x08
const INVALID_VHP = 0x09
const INVALID_KED = 0x0a
const INVALID_KEU = 0x0b
const INVALID_VHW = 0x0c
const TOO_BIG_BLOCK = 0x0d
const INVALID_SIGNATURE = 0x0e
const INVALID_NODE_ID = 0x0f
const INVALID_SHARD = 0x10
const INVALID_BUCKET_NAME = 0x11
const INVALID_OBJECT_NAME = 0x12
const TOO_MANY_BUCKETS = 0x13
const BUCKET_ALREADY_EXISTS = 0x14
const OBJECT_ALREADY_EXISTS = 0x15
const NODE_EXISTS = 0x16
const BUCKET_NOT_EMPTY = 0x17
const NO_ENOUGH_NODE = 0x18
const INVALID_NEXTFILENAME = 0x19
const INVALID_NEXTVERSIONID = 0x20
const INVALID_SESSION = 0x21
const NEED_LOGIN = 0x22
const INVALID_NEXTID = 0x23
const TOO_MANY_CURSOR = 0x24
const TOO_LOW_VERSION = 0x25
const DN_IN_BLACKLIST = 0x26
const BAD_MESSAGE = 0x27
const CACHE_FULL = 0x28
const INVALID_ARGS = 0x30
const CONN_ERROR = 0x31
const CODEC_ERROR = 0x32
const BAD_FILE = 0x33
const PRIKEY_NOT_EXIST = 0x34

var BUSY_ERROR = NewErrorMsg(SERVER_ERROR, "Too many routines")

func NewErrorMsg(code int32, msg string) *ErrorMessage {
	err := &ErrorMessage{}
	err.Code = code
	err.Msg = strings.TrimSpace(msg)
	return err
}

func ToError(err *ErrorMessage) error {
	return fmt.Errorf("ServiceError %d:%s", err.Code, strings.TrimSpace(err.Msg))
}

func NewError(code int32) *ErrorMessage {
	err := &ErrorMessage{}
	err.Code = code
	err.Msg = ""
	return err
}

func NewErrorMessage(bs []byte) *ErrorMessage {
	msg := UnmarshalMsg(bs)
	if errmsg, ok := msg.(*ErrorMessage); ok {
		return errmsg
	} else {
		return NewErrorMsg(SERVER_ERROR, "Unknown type cannot be cast to ErrorMessage")
	}
}

func ErrorMsg(code int32, msg string) []byte {
	err := &ErrorMessage{}
	err.Code = code
	err.Msg = strings.TrimSpace(msg)
	return MarshalError(err)
}

func MarshalError(msg *ErrorMessage) []byte {
	name := "ErrorMessage"
	msgType, _ := CLASS_ID_MAP[name]
	res, _ := proto.Marshal(msg)
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], msgType)
	return bytes.Join([][]byte{b[:], res}, []byte(""))
}
