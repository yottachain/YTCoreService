package codec

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"errors"
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

func GenerateRandomKey() []byte {
	h := rand.Uint64()
	l := rand.Uint64()
	var buf = make([]byte, 16)
	binary.BigEndian.PutUint64(buf, h)
	binary.BigEndian.PutUint64(buf[8:16], l)
	sha256Digest := sha256.New()
	sha256Digest.Write(buf)
	return sha256Digest.Sum(nil)
}

func GenerateUserKey(bs []byte) []byte {
	size := len(bs)
	if size > 32 {
		return bs[0:32]
	} else if size == 32 {
		return bs
	} else {
		siz := 32 - size
		bss := make([]byte, siz)
		return bytes.Join([][]byte{bs, bss}, []byte{})
	}
}

type BlockAESEncryptor struct {
	plainBlock *PlainBlock
	secretKey  []byte
}

func NewBlockAESEncryptor(b *PlainBlock, key []byte) *BlockAESEncryptor {
	bae := new(BlockAESEncryptor)
	bae.plainBlock = b
	bae.secretKey = key
	return bae
}

func (bae *BlockAESEncryptor) Encrypt() (*EncryptedBlock, error) {
	if bae.plainBlock.Data == nil {
		return nil, errors.New("data is null")
	}
	block, err := aes.NewCipher(bae.secretKey)
	if err != nil {
		return nil, err
	}
	srcData := PKCS7Padding(bae.plainBlock.Data, 16)
	blockMode := cipher.NewCBCEncrypter(block, IVParameter)
	dstData := make([]byte, len(srcData))
	blockMode.CryptBlocks(dstData, srcData)
	encryptedBlock := new(EncryptedBlock)
	encryptedBlock.SecretKey = bae.secretKey
	encryptedBlock.Data = dstData
	return encryptedBlock, nil
}

type BlockAESDecryptor struct {
	encryptedBlock *EncryptedBlock
}

func NewBlockAESDecryptor(b *EncryptedBlock) *BlockAESDecryptor {
	bae := new(BlockAESDecryptor)
	bae.encryptedBlock = b
	return bae
}

func (bae *BlockAESDecryptor) Decrypt() (*PlainBlock, error) {
	if bae.encryptedBlock.Data == nil {
		return nil, errors.New("data is null")
	}
	if bae.encryptedBlock.SecretKey == nil {
		return nil, errors.New("SecretKey is null")
	}
	length := len(bae.encryptedBlock.Data)
	if length%16 > 0 {
		return nil, errors.New("data err")
	}
	block, err := aes.NewCipher(bae.encryptedBlock.SecretKey)
	if err != nil {
		return nil, err
	}
	blockMode := cipher.NewCBCDecrypter(block, IVParameter)
	dstData := make([]byte, length)
	blockMode.CryptBlocks(dstData, bae.encryptedBlock.Data)
	plainBlock := new(PlainBlock)
	plainBlock.Data = PKCS7UnPadding(dstData)
	return plainBlock, nil
}

func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func ECBDecrypt(data, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	length := len(data)
	if length%16 > 0 {
		return data
	}
	decrypted := make([]byte, length)
	size := block.BlockSize()
	for bs, be := 0, size; bs < length; bs, be = bs+size, be+size {
		block.Decrypt(decrypted[bs:be], data[bs:be])
	}
	return PKCS7UnPadding(decrypted)
}

func ECBEncrypt(data, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	data = PKCS7Padding(data, block.BlockSize())
	length := len(data)
	if length%16 > 0 {
		return data
	}
	decrypted := make([]byte, length)
	size := block.BlockSize()
	for bs, be := 0, size; bs < length; bs, be = bs+size, be+size {
		block.Encrypt(decrypted[bs:be], data[bs:be])
	}
	return decrypted
}

func ECBDecryptNoPad(data, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	length := len(data)
	if length%16 > 0 {
		return data
	}
	decrypted := make([]byte, length)
	size := block.BlockSize()
	for bs, be := 0, size; bs < length; bs, be = bs+size, be+size {
		block.Decrypt(decrypted[bs:be], data[bs:be])
	}
	return decrypted
}

func ECBEncryptNoPad(data, key []byte) []byte {
	length := len(data)
	block, _ := aes.NewCipher(key)
	if length%16 > 0 {
		data = PKCS7Padding(data, block.BlockSize())
		length = len(data)
	}
	decrypted := make([]byte, length)
	size := block.BlockSize()
	for bs, be := 0, size; bs < length; bs, be = bs+size, be+size {
		block.Encrypt(decrypted[bs:be], data[bs:be])
	}
	return decrypted
}
