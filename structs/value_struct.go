package structs

import (
	"encoding/binary"
	"tiny-badger/utils"
)

type ValueStruct struct {
	Meta      byte
	UserMeta  byte
	ExpiresAt uint64
	Value     []byte

	Version uint64 // internal use, not enc/dec
}

func (v *ValueStruct) EncodedSize() uint32 {
	sz := len(v.Value) + 2
	enc := utils.SizeVarint(v.ExpiresAt)
	return uint32(sz + enc)
}

func (v *ValueStruct) Encode(buf []byte) uint32 {
	buf[0] = v.Meta
	buf[1] = v.UserMeta
	sz := binary.PutUvarint(buf[2:], v.ExpiresAt)
	n := copy(buf[2+sz:], v.Value)
	return uint32(2 + sz + n)
}

func (v *ValueStruct) Decode(buf []byte) {
	v.Meta = buf[0]
	v.UserMeta = buf[1]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(buf[2:])
	v.Value = buf[2+sz:]
}
