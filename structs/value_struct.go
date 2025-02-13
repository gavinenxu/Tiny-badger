package structs

import "encoding/binary"

type ValueStruct struct {
	Meta      byte
	ExpiresAt uint64
	Value     []byte
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func (v *ValueStruct) EncodedSize() uint32 {
	sz := len(v.Value) + 1
	enc := sizeVarint(v.ExpiresAt)
	return uint32(sz + enc)
}

func (v *ValueStruct) Encode(buf []byte) uint32 {
	buf[0] = v.Meta
	sz := binary.PutUvarint(buf[1:], v.ExpiresAt)
	n := copy(buf[1+sz:], v.Value)
	return uint32(1 + sz + n)
}

func (v *ValueStruct) Decode(buf []byte) {
	v.Meta = buf[0]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(buf[1:])
	v.Value = buf[1+sz:]
}
