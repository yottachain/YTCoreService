package codec

type ErasureDecoder struct {
	encryptedBlockSize int64
	decoder            LRC_Decoder
	ok                 bool
	copydata           []byte
}

func NewErasureDecoder(size int64) (*ErasureDecoder, error) {
	me := &ErasureDecoder{encryptedBlockSize: size}
	b := NeedLRCEncode(int(size))
	if b {
		lrc, err := LRC_Decode(size)
		if err != nil {
			return nil, err
		}
		me.decoder = lrc
	}
	me.ok = false
	return me, nil
}

func (me *ErasureDecoder) IsOK() bool {
	return me.ok
}

func (me *ErasureDecoder) GetEncryptedBlock() *EncryptedBlock {
	if me.decoder == nil {
		if me.copydata == nil {
			return nil
		}
		b := new(EncryptedBlock)
		b.Data = me.copydata
		return b
	}
	if me.decoder.GetOut() == nil {
		return nil
	}
	b := new(EncryptedBlock)
	b.Data = me.decoder.GetOut()
	return b
}

func (me *ErasureDecoder) AddShard(bs []byte) (bool, error) {
	if me.ok {
		return true, nil
	}
	if me.decoder == nil {
		me.copydata = bs[1 : me.encryptedBlockSize+1]
		me.ok = true
		return true, nil
	}
	bss, err := me.decoder.Decode(bs)
	if err != nil {
		return false, err
	}
	if bss == nil {
		return false, nil
	} else {
		me.ok = true
		return true, nil
	}
}
