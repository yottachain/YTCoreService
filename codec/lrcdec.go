package codec

type LRCDecoder struct {
	encryptedBlockSize int64
	decoder            *LRC_Decoder
}

func NewLRCDecoder(size int64) (*LRCDecoder, error) {
	me := &LRCDecoder{encryptedBlockSize: size}
	lrc, err := LRC_Decode(size)
	if err != nil {
		return nil, err
	}
	me.decoder = lrc
	return me, nil
}

func (me *LRCDecoder) GetEncryptedBlock() *EncryptedBlock {
	if me.decoder.GetOut() == nil {
		return nil
	}
	b := new(EncryptedBlock)
	b.Data = me.decoder.GetOut()
	return b
}

func (me *LRCDecoder) AddShard(bs []byte) (bool, error) {
	bss, err := me.decoder.Decode(bs)
	if err != nil {
		return false, err
	}
	if bss == nil {
		return false, nil
	} else {
		return true, nil
	}
}
