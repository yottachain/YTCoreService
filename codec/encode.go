package codec

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/aurawing/eos-go/btcsuite/btcutil/base58"
	"github.com/yottachain/YTCoreService/env"
	"github.com/yottachain/YTCoreService/pkt"
)

type Encoder struct {
	key           string
	userId        uint32
	keyNumber     uint32
	storeNumber   uint32
	sign          string
	checker       DupBlockChecker
	fc            *FileEncoder
	ReadinLength  *env.AtomInt64
	ReadOutLength *env.AtomInt64
	WriteLength   *env.AtomInt64
}

func NewEncoder(uid, keyNum, storeNum uint32, signstr string, s3key string, enc *FileEncoder, check DupBlockChecker) *Encoder {
	return &Encoder{key: s3key,
		userId:      uid,
		keyNumber:   keyNum,
		storeNumber: storeNum,
		sign:        signstr,
		checker:     check,
		fc:          enc,
	}
}

func (enc *Encoder) GetSHA256() []byte {
	return enc.fc.GetVHW()
}

func (enc *Encoder) GetBaseSHA256() string {
	return base58.Encode(enc.fc.GetVHW())
}

func (enc *Encoder) GetMD5() []byte {
	return enc.fc.GetMD5()
}

func (enc *Encoder) GetBaseMD5() string {
	return base58.Encode(enc.fc.GetMD5())
}

func (enc *Encoder) HandleProgress(Readin, ReadOut, Write *env.AtomInt64) {
	enc.ReadinLength = Readin
	enc.ReadOutLength = ReadOut
	enc.WriteLength = Write
}

func (enc *Encoder) Handle(out string) *pkt.ErrorMessage {
	f, err := os.OpenFile(out, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	defer f.Close()
	size, err := enc.writeHead(f)
	if err != nil {
		return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
	}
	var lastpos int64 = size
	id := 0
	for {
		b, err := enc.fc.ReadNext()
		if err != nil {
			return pkt.NewErrorMsg(pkt.CODEC_ERROR, err.Error())
		}
		id++
		if enc.ReadinLength != nil {
			enc.ReadinLength.Add(enc.fc.GetReadinTotal())
			enc.ReadOutLength.Add(enc.fc.GetReadoutTotal())
		}
		if b == nil {
			break
		} else {
			obj, err := enc.checker.Check(b, id)
			if err != nil {
				return err
			}
			var werr error
			if obj.IsDup {
				size, werr = enc.writeDupBlock(f, obj)
			} else {
				size, werr = enc.writeNoDupBlock(f, obj)
			}
			if werr != nil {
				return pkt.NewErrorMsg(pkt.SERVER_ERROR, werr.Error())
			}
			if enc.WriteLength != nil {
				enc.WriteLength.Add(b.Length())
			}
			lastpos = lastpos + size
		}
	}
	err = enc.writeKey(f)
	if err != nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	err = enc.writeBottonPos(f, lastpos)
	if err != nil {
		return pkt.NewErrorMsg(pkt.SERVER_ERROR, err.Error())
	}
	return nil
}

func (enc *Encoder) writeDupBlock(f *os.File, b *EncodedBlock) (int64, error) {
	bs1 := []byte{0x01}
	bs2 := env.IdToBytes(b.OriginalSize)
	bs3 := env.IdToBytes(b.RealSize)
	bss := bytes.Join([][]byte{bs1, bs2, bs3, b.VHP, b.KEU, b.VHB}, []byte{})
	_, err := f.Write(bss)
	if err != nil {
		return 0, err
	}
	return 1 + 8 + 8 + 32 + 32 + 16, nil
}

func (enc *Encoder) writeNoDupBlock(f *os.File, b *EncodedBlock) (int64, error) {
	bs1 := []byte{0x00}
	bs2 := env.IdToBytes(b.OriginalSize)
	bs3 := env.IdToBytes(b.RealSize)
	size := int64(len(b.DATA))
	bs4 := env.IdToBytes(size)
	bss := bytes.Join([][]byte{bs1, bs2, bs3, bs4, b.VHP, b.KEU, b.KED, b.DATA}, []byte{})
	_, err := f.Write(bss)
	if err != nil {
		return 0, err
	}
	return 1 + 8 + 8 + 8 + 32 + 32 + 32 + size, nil
}

func (enc *Encoder) writeHead(f *os.File) (int64, error) {
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, int64(0))
	binary.Write(bytebuf, binary.BigEndian, enc.fc.GetLength())
	bytebuf.Write(enc.GetMD5())
	binary.Write(bytebuf, binary.BigEndian, enc.userId)
	binary.Write(bytebuf, binary.BigEndian, enc.keyNumber)
	binary.Write(bytebuf, binary.BigEndian, enc.storeNumber)
	bs7 := []byte(enc.sign)
	size := len(bs7)
	binary.Write(bytebuf, binary.BigEndian, int32(size))
	bytebuf.Write(bs7)
	bss := bytebuf.Bytes()
	_, err := f.Write(bss)
	if err != nil {
		return 0, err
	}
	return 8 + 8 + 16 + 4 + 4 + 4 + 4 + int64(size), nil
}

func (enc *Encoder) writeKey(f *os.File) error {
	return WriteKey(enc.key, f)
}

func (enc *Encoder) writeBottonPos(f *os.File, pos int64) error {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	bs := env.IdToBytes(pos)
	_, err = f.Write(bs)
	if err != nil {
		return err
	}
	return nil
}

func Append(s3key, path string) error {
	key := s3key
	exist, err := checkExist(key, path)
	if err != nil {
		return err
	}
	if !exist {
		writeAppend(key, path)
	}
	return nil
}

func writeAppend(key, path string) error {
	f, err := os.OpenFile(path, os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	err = WriteKey(key, f)
	if err != nil {
		return err
	}
	return nil
}

func checkExist(key, path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	pos, err := ReadInt64(f)
	if err != nil {
		return false, err
	}
	_, err = f.Seek(pos, io.SeekStart)
	if err != nil {
		return false, err
	}
	reader := bufio.NewReader(f)
	for {
		size, err := ReadInt32(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return false, err
		}
		bs := make([]byte, size)
		err = ReadFull(reader, bs)
		if err != nil {
			return false, err
		}
		s := string(bs)
		if s == key {
			return true, nil
		}
	}
	return false, nil
}

func WriteKey(key string, f *os.File) error {
	bs := []byte(key)
	size := int32(len(bs))
	bs1 := env.Int32ToBytes(size)
	bss := bytes.Join([][]byte{bs1, bs}, []byte{})
	_, err := f.Write(bss)
	if err != nil {
		return err
	}
	return nil
}
