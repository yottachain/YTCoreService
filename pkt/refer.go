package pkt

import (
	"bytes"
)

func ReferIds(ls []*Refer) []uint32 {
	if ls == nil {
		return []uint32{}
	}
	count := len(ls)
	nums := make([]uint32, count)
	for index, bs := range ls {
		nums[index] = uint32(bs.Id)
	}
	return nums
}

func ParseRefers(blks [][]byte) []*Refer {
	if blks == nil {
		return []*Refer{}
	}
	count := len(blks)
	refs := make([]*Refer, count)
	for index, bs := range blks {
		refs[index] = NewRefer(bs)
	}
	return refs
}

func MergeRefers(ls []*Refer) [][]byte {
	if ls == nil {
		return [][]byte{}
	}
	count := len(ls)
	blks := make([][]byte, count)
	for index, ref := range ls {
		blks[index] = ref.Bytes()
	}
	return blks
}

type Refer struct {
	VBI          int64
	SuperID      uint8
	OriginalSize int64
	RealSize     int32
	KEU          []byte
	KeyNumber    int16
	Id           int16
}

func NewRefer(bs []byte) *Refer {
	if bs == nil {
		return nil
	}
	size := len(bs)
	if !(len(bs) == 54 || len(bs) == 167) {
		return nil
	}
	vbi := int64(bs[0] & 0xFF)
	vbi = vbi<<8 | int64(bs[1]&0xFF)
	vbi = vbi<<8 | int64(bs[2]&0xFF)
	vbi = vbi<<8 | int64(bs[3]&0xFF)
	vbi = vbi<<8 | int64(bs[4]&0xFF)
	vbi = vbi<<8 | int64(bs[5]&0xFF)
	vbi = vbi<<8 | int64(bs[6]&0xFF)
	vbi = vbi<<8 | int64(bs[7]&0xFF)
	supid := uint8(bs[8])
	originalSize := int64(bs[9] & 0xFF)
	originalSize = originalSize<<8 | int64(bs[10]&0xFF)
	originalSize = originalSize<<8 | int64(bs[11]&0xFF)
	originalSize = originalSize<<8 | int64(bs[12]&0xFF)
	originalSize = originalSize<<8 | int64(bs[13]&0xFF)
	originalSize = originalSize<<8 | int64(bs[14]&0xFF)
	realSize := int32(bs[15] & 0xFF)
	realSize = realSize<<8 | int32(bs[16]&0xFF)
	realSize = realSize<<8 | int32(bs[17]&0xFF)
	if size == 54 {
		keu := bs[18 : 18+32]
		id := int16(bs[50] & 0xFF)
		id = id<<8 | int16(bs[51]&0xFF)
		KeyNumber := int16(bs[52] & 0xFF)
		KeyNumber = KeyNumber<<8 | int16(bs[53]&0xFF)
		return &Refer{vbi, supid, originalSize, realSize, keu, KeyNumber, id}
	} else {
		keu := bs[18 : 18+145]
		id := int16(bs[18+145] & 0xFF)
		id = id<<8 | int16(bs[18+145+1]&0xFF)
		KeyNumber := int16(bs[18+145+3] & 0xFF)
		KeyNumber = KeyNumber<<8 | int16(bs[18+145+3]&0xFF)
		return &Refer{vbi, supid, originalSize, realSize, keu, KeyNumber, id}
	}
}

func (self *Refer) Bytes() []byte {
	bs := make([]byte, 18)
	bs[0] = uint8(self.VBI >> 56)
	bs[1] = uint8(self.VBI >> 48)
	bs[2] = uint8(self.VBI >> 40)
	bs[3] = uint8(self.VBI >> 32)
	bs[4] = uint8(self.VBI >> 24)
	bs[5] = uint8(self.VBI >> 16)
	bs[6] = uint8(self.VBI >> 8)
	bs[7] = uint8(self.VBI)
	bs[8] = self.SuperID
	bs[9] = uint8(self.OriginalSize >> 40)
	bs[10] = uint8(self.OriginalSize >> 32)
	bs[11] = uint8(self.OriginalSize >> 24)
	bs[12] = uint8(self.OriginalSize >> 16)
	bs[13] = uint8(self.OriginalSize >> 8)
	bs[14] = uint8(self.OriginalSize)
	bs[15] = uint8(self.RealSize >> 16)
	bs[16] = uint8(self.RealSize >> 8)
	bs[17] = uint8(self.RealSize)
	bs1 := make([]byte, 4)
	bs1[0] = uint8(self.Id >> 8)
	bs1[1] = uint8(self.Id)
	bs1[2] = uint8(self.KeyNumber >> 8)
	bs1[3] = uint8(self.KeyNumber)
	return bytes.Join([][]byte{bs, self.KEU, bs1}, []byte(""))
}
