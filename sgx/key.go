package sgx

import (
	"bytes"
	"errors"

	"github.com/eoscanada/eos-go/btcsuite/btcutil/base58"
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
