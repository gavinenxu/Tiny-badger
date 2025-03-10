package structs

import (
	"encoding/binary"
)

const (
	MaxHeaderSize = 22
)

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64
	Meta      byte
}

func NewEntry(key, val []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: val,
	}
}

func (e *Entry) WithMeta(meta byte) *Entry {
	e.Meta = meta
	return e
}

type ValuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint32
}

// Header
// +----+------+------+---------+
// |Meta|KeyLen|ValLen|ExpiresAt|
// +----+------+------+---------+
// meta is invariant size, others are variant size
type Header struct {
	KeyLen    uint32
	ValLen    uint32
	ExpiresAt uint64
	Meta      byte
}

func (h *Header) Encode(buf []byte) int {
	buf[0] = h.Meta
	idx := 1
	idx += binary.PutUvarint(buf[idx:], uint64(h.KeyLen))
	idx += binary.PutUvarint(buf[idx:], uint64(h.ValLen))
	idx += binary.PutUvarint(buf[idx:], h.ExpiresAt)
	return idx
}

func (h *Header) Decode(buf []byte) int {
	h.Meta = buf[0]
	idx := 1
	kLen, cnt := binary.Uvarint(buf[idx:])
	h.KeyLen = uint32(kLen)
	idx += cnt
	vLen, cnt := binary.Uvarint(buf[idx:])
	h.ValLen = uint32(vLen)
	idx += cnt
	h.ExpiresAt, cnt = binary.Uvarint(buf[idx:])
	return idx + cnt
}
