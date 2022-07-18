package sgx

import (
	"bytes"
	"crypto/aes"
	"errors"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/yottachain/YTCrypto"
)

type Key struct {
	PrivateKey string
	KeyNumber  uint32
	AESKey     []byte
}

func NewKey(privkey string, number uint32) (*Key, error) {
	k := &Key{PrivateKey: privkey, KeyNumber: number}
	bs := base58.Decode(privkey)
	if len(bs) != 37 {
		return nil, errors.New("Invalid private key " + privkey)
	}
	k.AESKey = GenerateUserKey(bs)
	return k, nil
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

func (k *Key) Decrypt(data []byte) []byte {
	if len(data) == 32 {
		return k.ECBDecryptNoPad(data)
	} else {
		return k.ECCDecrypt(data)
	}
}

func (k *Key) ECCDecrypt(data []byte) []byte {
	src, err := YTCrypto.ECCDecrypt(data, k.PrivateKey)
	if err != nil {
		return data
	}
	return src
}

func (k *Key) ECBDecryptNoPad(data []byte) []byte {
	block, _ := aes.NewCipher(k.AESKey)
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
