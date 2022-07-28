package sgx

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"time"
)

var IVParameter []byte

func init() {
	bs := []byte("YottaChain2018王东临侯月文韩大光")
	md5Digest := md5.New()
	md5Digest.Write(bs)
	IVParameter = md5Digest.Sum(nil)
	rand.Seed(time.Now().UnixNano())
}

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

func (b *EncryptedBlock) ToBytes() []byte {
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, b.KeyNumber)
	keusize := int16(len(b.KEU))
	binary.Write(bytebuf, binary.BigEndian, keusize)
	datasize := int32(len(b.DATA))
	binary.Write(bytebuf, binary.BigEndian, datasize)
	bytebuf.Write(b.KEU)
	bytebuf.Write(b.DATA)
	return bytebuf.Bytes()
}

func (b *EncryptedBlock) Decrypt(key *Key) ([]byte, error) {
	if b.DATA == nil {
		return nil, errors.New("sgx decrypt:data is null")
	}
	if b.KeyNumber != int32(key.KeyNumber) {
		return nil, errors.New("KeyNumber err")
	}
	bs := key.Decrypt(b.KEU)
	length := len(b.DATA)
	if length%16 > 0 {
		return nil, errors.New("data err")
	}
	block, err := aes.NewCipher(bs)
	if err != nil {
		return nil, err
	}
	blockMode := cipher.NewCBCDecrypter(block, IVParameter)
	dstData := make([]byte, length)
	blockMode.CryptBlocks(dstData, b.DATA)
	return PKCS7UnPadding(dstData), nil
}

func (b *EncryptedBlock) Decode(key *Key, writer io.Writer) error {
	pdata, err := b.Decrypt(key)
	if err != nil {
		return errors.New("Decrypt err")
	}
	read, err := NewBlockReader(pdata)
	if err != nil {
		return errors.New("Decrypt err")
	}
	readbuf := make([]byte, 8192)
	for {
		num, err := read.Read(readbuf)
		if err != nil && err != io.EOF {
			return err
		}
		if num > 0 {
			bs := readbuf[0:num]
			writer.Write(bs)
		}
		if err != nil && err == io.EOF {
			break
		}
	}
	return nil
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}
