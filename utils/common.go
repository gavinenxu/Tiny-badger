package utils

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

var (
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

// CompareKeys without timestamp (8 bit), if equal then compare ts
func CompareKeys(key1, key2 []byte) int {
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(out)-8:], ts)
	return out
}

func ParseTs(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(key[len(key)-8:])
}

func SizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
