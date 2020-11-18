package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/yottachain/YTCoreService/env"
)

type Key struct {
	UserID     int32
	Bucket     string
	ObjectName string
}

func (self *Key) ToBytes() []byte {
	s := self.Bucket + "/" + self.ObjectName
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, self.UserID)
	bytebuf.Write([]byte(s))
	return bytebuf.Bytes()
}

func NewKey(data []byte) *Key {
	ii := env.BytesToInt32(data[0:4])
	ss := string(data[4:])
	pos := strings.Index(ss, "/")
	buck := ss[0:pos]
	name := ss[pos:]
	k := &Key{UserID: ii, Bucket: buck, ObjectName: name}
	return k
}

type Value struct {
	Type   int8
	Length int64
	Md5    []byte
	Path   []string
	Data   []byte
}

func MultiPartFileValue(path []string, length int64, md5 []byte) *Value {
	return &Value{Type: 2, Length: length, Md5: md5, Path: path}
}

func SingleFileValue(path string, length int64, md5 []byte) *Value {
	return &Value{Type: 1, Length: length, Md5: md5, Path: []string{path}}
}

func BytesFileValue(data []byte, length int64, md5 []byte) *Value {
	return &Value{Type: 0, Length: length, Md5: md5, Path: []string{}, Data: data}
}

func NewValue(data []byte) *Value {
	v := &Value{}
	bytebuf := bytes.NewBuffer(data)
	binary.Read(bytebuf, binary.BigEndian, &v.Type)
	binary.Read(bytebuf, binary.BigEndian, &v.Length)
	v.Md5 = make([]byte, 16)
	bytebuf.Read(v.Md5)
	if v.Type == 0 {
		size := int32(0)
		binary.Read(bytebuf, binary.BigEndian, &size)
		v.Data = make([]byte, size)
		bytebuf.Read(v.Data)
	}
	size := int32(0)
	binary.Read(bytebuf, binary.BigEndian, &size)
	v.Path = make([]string, size)
	for i := 0; i < int(size); i++ {
		size := int32(0)
		binary.Read(bytebuf, binary.BigEndian, &size)
		bs := make([]byte, size)
		bytebuf.Read(bs)
		v.Path[i] = string(bs)
	}
	return v
}

func (self *Value) ToBytes() []byte {
	bytebuf := bytes.NewBuffer([]byte{})
	if self.Data != nil {
		self.Type = 0
	} else {
		if len(self.Path) == 1 {
			self.Type = 1
		} else {
			self.Type = 2
		}
	}
	binary.Write(bytebuf, binary.BigEndian, self.Type)
	binary.Write(bytebuf, binary.BigEndian, self.Length)
	bytebuf.Write(self.Md5)
	if self.Type == 0 {
		ii := int32(len(self.Data))
		binary.Write(bytebuf, binary.BigEndian, ii)
		bytebuf.Write(self.Data)
	}
	i := int32(len(self.Path))
	binary.Write(bytebuf, binary.BigEndian, i)
	for _, ss := range self.Path {
		bs := []byte(ss)
		ii := int32(len(bs))
		binary.Write(bytebuf, binary.BigEndian, ii)
		bytebuf.Write(bs)
	}
	return bytebuf.Bytes()
}

func SumSpace() int64 {
	return 0
}

func InsertValue(k *Key, v *Value) error {
	return DB.Update(func(tx *bolt.Tx) error {
		bs := k.ToBytes()
		vv := TempBuck.Get(bs)
		if vv != nil {
			return errors.New("Repeat upload")
		}
		err := TempBuck.Put(bs, v.ToBytes())
		if err != nil {
			return err
		}
		return nil
	})
}

func DeleteValue(k *Key) {
	DB.Update(func(tx *bolt.Tx) error {
		TempBuck.Delete(k.ToBytes())
		return nil
	})
}

func GetValue(userid int32, buck, key string) *Value {
	Key := &Key{UserID: userid,
		Bucket:     buck,
		ObjectName: key}
	var val []byte
	DB.View(func(tx *bolt.Tx) error {
		val = TempBuck.Get(Key.ToBytes())
		return nil
	})
	if val == nil || len(val) == 0 {
		return nil
	}
	return NewValue(val)
}
