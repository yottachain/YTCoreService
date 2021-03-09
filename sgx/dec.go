package sgx

import (
	"bytes"
	"encoding/binary"
)

type EncryptedBlock struct {
	DATA      []byte
	KEU       []byte
	KeyNumber int32
}

func NewEncryptedBlock(bs []byte) *EncryptedBlock {
	eb := &EncryptedBlock{KeyNumber: 0}
	headbuf := bytes.NewBuffer(bs)
	binary.Read(headbuf, binary.BigEndian, &eb.KeyNumber)
	keusize := int16(0)
	binary.Read(headbuf, binary.BigEndian, &keusize)
	datasize := int32(0)
	binary.Read(headbuf, binary.BigEndian, &datasize)
	eb.KEU = make([]byte, keusize)
	headbuf.Read(eb.KEU)
	eb.DATA = make([]byte, datasize)
	headbuf.Read(eb.DATA)
	return eb
}

func (self *EncryptedBlock) ToBytes() []byte {
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, self.KeyNumber)
	keusize := int16(len(self.KEU))
	binary.Write(bytebuf, binary.BigEndian, keusize)
	datasize := int32(len(self.DATA))
	binary.Write(bytebuf, binary.BigEndian, datasize)
	bytebuf.Write(self.KEU)
	bytebuf.Write(self.DATA)
	return bytebuf.Bytes()
}
