package skl

import (
	"sync/atomic"
	"tiny-badger/structs"
	"tiny-badger/utils"
	"unsafe"
)

const (
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
)

// Arena Replace a pointer in skiplist to a contiguous memory region, and accessed by offset
type Arena struct {
	n   atomic.Uint32
	buf []byte
}

func newArena(n int64) *Arena {
	a := &Arena{buf: make([]byte, n)}
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	a.n.Store(1)
	return a
}

func (a *Arena) size() int64 {
	return int64(a.n.Load())
}

// put node in arena according to its height
func (a *Arena) putNode(height int) uint32 {
	unusedSize := (maxHeight - height) * offsetSize
	// Pad the allocation with enough bytes to ensure the requested alignment.
	padded := uint32(MaxNodeSize - unusedSize + nodeAlign)
	newSize := a.n.Add(padded)
	utils.AssertTruef(int(newSize) <= len(a.buf), "Arena too small, toWrite:%d newTotal:%d limit:%d", padded, newSize, len(a.buf))

	// get the aligned offset, e,x (1 + 7) & (^7) => 8 & (11110000)  = 8
	offset := (newSize - padded + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return offset
}

func (a *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}

	return (*node)(unsafe.Pointer(&a.buf[offset]))
}

func (a *Arena) getNodeOffset(n *node) uint32 {
	if n == nil {
		return 0
	}
	// offset = node.ptr - startBuf.ptr
	return uint32(uintptr(unsafe.Pointer(n)) - uintptr(unsafe.Pointer(&a.buf[0])))
}

func (a *Arena) putKey(key []byte) uint32 {
	size := uint32(len(key))
	newSize := a.n.Add(size)
	utils.AssertTruef(int(newSize) <= len(a.buf), "Arena too small, toWrite:%d newTotal:%d limit:%d", size, newSize, len(a.buf))

	offset := newSize - size
	// copy key to buf
	utils.AssertTrue(len(key) == copy(a.buf[offset:newSize], key))
	return offset
}

func (a *Arena) getKey(offset uint32, size uint16) []byte {
	return a.buf[offset : offset+uint32(size)]
}

func (a *Arena) putValue(value structs.ValueStruct) uint32 {
	size := value.EncodedSize()
	newSize := a.n.Add(size)
	utils.AssertTruef(int(newSize) <= len(a.buf), "Arena too small, toWrite:%d newTotal:%d limit:%d", size, newSize, len(a.buf))

	offset := newSize - size
	// copy value to buf
	value.Encode(a.buf[offset:])
	return offset
}

func (a *Arena) getValue(offset uint32, size uint32) (v structs.ValueStruct) {
	v.Decode(a.buf[offset : offset+size])
	return
}
