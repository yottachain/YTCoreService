package api

import (
	"bytes"
	"errors"
	"io"

	"github.com/yottachain/YTCoreService/codec"
	"github.com/yottachain/YTCoreService/pkt"
)

type BackupCaller interface {
	GetBackupReader(pos int64) io.ReadCloser
	GetAESKey() []byte
}

type DownLoadReader struct {
	UClient    *Client
	BkCall     BackupCaller
	Refs       map[int32]*pkt.Refer
	readpos    int64
	pos        int64
	end        int64
	referIndex int32
	bin        io.Reader
}

func NewDownLoadReader(dobj *DownloadObject, st, ed int64) *DownLoadReader {
	reader := &DownLoadReader{UClient: dobj.UClient, readpos: st, end: ed, BkCall: dobj.BkCall, referIndex: 0, pos: 0}
	refmap := make(map[int32]*pkt.Refer)
	for _, ref := range dobj.REFS {
		id := int32(ref.Id) & 0xFFFF
		refmap[id] = ref
	}
	reader.Refs = refmap
	return reader
}

func (me *DownLoadReader) readBlock() error {
	if me.bin != nil {
		_, ok := me.bin.(*codec.BlockReader)
		if ok {
			me.bin = nil
		}
	}
	for {
		if me.bin != nil {
			break
		}
		refer := me.Refs[me.referIndex]
		if refer == nil {
			return nil
		}
		if me.readpos < me.pos+refer.OriginalSize {
			dn := &DownloadBlock{UClient: me.UClient, Ref: refer}
			plainblock, err := dn.Load()
			if err != nil {
				return me.ReadCaller(pkt.ToError(err))
			}
			if plainblock != nil {
				rd := codec.NewBlockReader(plainblock)
				er := rd.Skip(me.readpos - me.pos)
				if er != nil {
					return me.ReadCaller(er)
				}
				me.bin = rd
			}
		}
		me.pos = me.pos + refer.OriginalSize
		me.referIndex++
	}
	return nil
}

func (me *DownLoadReader) ReadCaller(err error) error {
	if me.BkCall == nil {
		return err
	}
	startpos := me.readpos / 16
	skipn := me.readpos % 16
	aes := NewAESDecodeReader(me.BkCall, startpos*16)
	err1 := aes.Skip(skipn)
	if err1 != nil {
		return err1
	}
	me.bin = aes
	return nil
}

func (me *DownLoadReader) Read(p []byte) (n int, err error) {
	if me.Refs == nil {
		return 0, errors.New("Stream closed")
	}
	if me.readpos >= me.end {
		return 0, io.EOF
	}
	if me.bin == nil {
		err := me.readBlock()
		if err != nil {
			return 0, err
		}
		if me.bin == nil {
			return 0, io.EOF
		}
	}
	count, err := me.bin.Read(p)
	if err != nil && err != io.EOF {
		return 0, err
	} else {
		if count > 0 {
			me.readpos = me.readpos + int64(count)
			if me.readpos > me.end {
				count = count - int(me.readpos-me.end)
			}
			return count, nil
		}
	}
	_, ok := me.bin.(*AESDecodeReader)
	if ok {
		me.readpos = me.readpos + int64(count)
		if me.readpos > me.end {
			count = count - int(me.readpos-me.end)
		}
		return count, err
	}
	err = me.readBlock()
	if err != nil {
		return 0, err
	}
	count, err = me.bin.Read(p)
	if count > 0 {
		me.readpos = me.readpos + int64(count)
		if me.readpos > me.end {
			count = count - int(me.readpos-me.end)
		}
	}
	return count, err
}

func (me *DownLoadReader) Close() error {
	me.Refs = nil
	if me.bin != nil {
		aes, ok := me.bin.(*AESDecodeReader)
		if ok {
			aes.Close()
		}
		me.bin = nil
	}
	return nil
}

const bufLen = 8192

type AESDecodeReader struct {
	buf    []byte
	pos    int
	lastbs []byte
	rd     io.ReadCloser
	key    []byte
	eof    bool
}

func NewAESDecodeReader(BkCall BackupCaller, startpos int64) *AESDecodeReader {
	aes := &AESDecodeReader{rd: BkCall.GetBackupReader(startpos)}
	aes.key = BkCall.GetAESKey()
	aes.pos = -1
	return aes
}

func (me *AESDecodeReader) Read(p []byte) (n int, err error) {
	if me.pos < 0 {
		err := me.Fill()
		if err != nil {
			return 0, err
		}
	} else {
		if me.pos >= len(me.buf) {
			err := me.Fill()
			if err != nil {
				return 0, err
			}
		}
	}
	count := len(p)
	remain := len(me.buf) - me.pos
	if count >= remain {
		for i := 0; i < remain; i++ {
			p[i] = me.buf[me.pos+i]
		}
		me.pos = -1
		return remain, nil
	} else {
		for i := 0; i < count; i++ {
			p[i] = me.buf[me.pos+i]
		}
		me.pos = me.pos + count
		return count, nil
	}
}

func (me *AESDecodeReader) Fill() error {
	bs, err := me.ReadBuf()
	if err != nil {
		return err
	}
	if bs == nil || len(bs) == 0 {
		if me.lastbs == nil {
			return io.EOF
		}
		me.buf = codec.ECBDecrypt(me.lastbs, me.key)
		me.lastbs = nil
	} else {
		count := len(bs)
		if count%16 > 0 {
			return errors.New("err data")
		}
		if me.eof {
			if me.lastbs == nil {
				me.buf = codec.ECBDecrypt(bs, me.key)
			} else {
				bss := bytes.Join([][]byte{me.lastbs, bs}, []byte{})
				me.buf = codec.ECBDecrypt(bss, me.key)
				me.lastbs = nil
			}
		} else {
			if me.lastbs == nil {
				me.buf = codec.ECBDecryptNoPad(bs[0:count-16], me.key)
				me.lastbs = bs[count-16:]
			} else {
				bss := bytes.Join([][]byte{me.lastbs, bs[0 : count-16]}, []byte{})
				me.buf = codec.ECBDecryptNoPad(bss, me.key)
				me.lastbs = bs[count-16:]
			}
		}
	}
	me.pos = 0
	return nil
}

func (me *AESDecodeReader) ReadBuf() ([]byte, error) {
	var bs []byte
	if me.eof {
		return nil, nil
	}
	remain := bufLen
	for {
		out := make([]byte, remain)
		num, err := me.rd.Read(out)
		if err != nil {
			if err != io.EOF {
				return nil, err
			} else {
				if num > 0 {
					if bs == nil {
						bs = out[0:num]
					} else {
						bs = bytes.Join([][]byte{bs, out[0:num]}, []byte{})
					}
				}
				me.eof = true
				return bs, nil
			}
		} else {
			if num > 0 {
				if bs == nil {
					bs = out[0:num]
				} else {
					bs = bytes.Join([][]byte{bs, out[0:num]}, []byte{})
				}
			}
		}
		remain = remain - num
		if remain == 0 {
			return bs, nil
		}
	}
}

func (me *AESDecodeReader) Close() error {
	me.rd.Close()
	return nil
}

func (me *AESDecodeReader) Skip(n int64) error {
	if n <= 0 {
		return nil
	}
	remain := n
	for {
		out := make([]byte, remain)
		reasn, err := me.Read(out)
		if err != nil && err != io.EOF {
			return err
		}
		remain = remain - int64(reasn)
		if remain == 0 {
			return nil
		}
	}
}
