package skl

import (
	"github.com/dgraph-io/ristretto/v2/z"
	"math"
	"sync/atomic"
	"tiny-badger/structs"
	"tiny-badger/utils"
	"unsafe"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

const MaxNodeSize = int(unsafe.Sizeof(node{}))

type node struct {
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value atomic.Uint64

	keyOffset uint32
	keySize   uint16

	height uint16

	// store next node offset for a specific height, the node's key is exactly larger than current node's key
	tower [maxHeight]atomic.Uint32
}

type Skiplist struct {
	height  atomic.Int32 // skiplist level's height
	arena   *Arena
	head    *node
	ref     atomic.Int32
	onClose func()
}

func newNode(arena *Arena, key []byte, val structs.ValueStruct, height int) *node {
	// allocate memory for node,key,value in arena, store the meta value in node struct
	offset := arena.putNode(height)
	n := arena.getNode(offset)
	n.keyOffset = arena.putKey(key)
	n.keySize = uint16(len(key))
	n.height = uint16(height)
	// store value offset (0-31) + size (48-63)
	n.value.Store(encodeValue(arena.putValue(val), val.EncodedSize()))
	return n
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valOffset)<<32 | uint64(valSize)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value >> 32)
	valSize = uint32(value)
	return
}

func (n *node) getKey(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *node) getValue(arena *Arena) structs.ValueStruct {
	valOff, valSize := n.getValueOffsetAndSize()
	return arena.getValue(valOff, valSize)
}

// setValue put value in arena and store it in node
func (n *node) setValue(arena *Arena, val structs.ValueStruct) {
	offset := arena.putValue(val)
	v := encodeValue(offset, val.EncodedSize())
	n.value.Store(v)
}

// get offset in arena
func (n *node) getNextOffset(height int) uint32 {
	return n.tower[height].Load()
}

func (n *node) getValueOffsetAndSize() (uint32, uint32) {
	value := n.value.Load()
	return decodeValue(value)
}

// update prev node's tower offset to the newly created node offset
func (n *node) casNextOffset(height int, old, newOffset uint32) bool {
	return n.tower[height].CompareAndSwap(old, newOffset)
}

func NewSkiplist(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, structs.ValueStruct{}, maxHeight)
	s := &Skiplist{arena: arena, head: head}
	s.height.Store(1) // initial height is 1
	s.ref.Store(1)    // initial ref count
	return s
}

func (s *Skiplist) IncrRef() {
	s.ref.Add(1)
}

func (s *Skiplist) DecrRef() {
	refCnt := s.ref.Add(-1)
	if refCnt > 0 {
		return
	}
	// reference count is 0
	if s.onClose != nil {
		s.onClose()
	}

	// clean up resource
	s.arena = nil
	s.head = nil
}

func (s *Skiplist) Put(key []byte, val structs.ValueStruct) {

	listHeight := s.getHeight()
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node
	prev[listHeight] = s.head
	next[listHeight] = nil
	for i := int(listHeight) - 1; i >= 0; i-- {
		// top-down to set prev and next for the key, and before node starts from list head node
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			// The case we found the node's key equals to the key passed in, we just need to update the value in arena
			prev[i].setValue(s.arena, val)
			return
		}
	}

	// create a new node for a random height, and there is less chance to get a higher height
	height := s.randHeight()
	x := newNode(s.arena, key, val, height)

	listHeight = s.getHeight()
	for height > int(listHeight) {
		// increase s.height to height via CAS
		if s.height.CompareAndSwap(listHeight, int32(height)) {
			break
		}
		listHeight = s.getHeight()
	}

	// insert from the base level up to the height - 1 for the new node
	for i := 0; i < height; i++ {
		for {
			if prev[i] == nil {
				utils.AssertTrue(i > 1) // can't happen in base level

				prev[i], next[i] = s.findSpliceForLevel(key, s.head, i) // while height exceeds the original list height, not compute the prev, next list, search from s.head

				utils.AssertTrue(prev[i] != next[i])
			}

			// link: prev[i].offset -> x.offset -> next[i].offset
			// prev next node offset, if next[i] is nil, we will set this node as
			nextNodeOffset := s.arena.getNodeOffset(next[i])
			// update the next offset in the tower of this node
			x.tower[i].Store(nextNodeOffset)
			// update prev node offset to new node offset, also could update s.head tower's offset for specifically height
			// because prev[i] node will exist at different thread, so we use CAS to update its value
			if prev[i].casNextOffset(i, nextNodeOffset, s.arena.getNodeOffset(x)) {
				break
			}

			// Note: CAS fails, we should recompute the prev and next for the height
			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				utils.AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				prev[i].setValue(s.arena, val)
				return
			}
		}
	}
}

func (s *Skiplist) Get(key []byte) structs.ValueStruct {
	n, eq := s.findNear(key, false, true) // >=
	if !eq {
		return structs.ValueStruct{}
	}

	return n.getValue(s.arena)
}

func (s *Skiplist) IsEmpty() bool {
	return s.findLast() == nil
}

func (s *Skiplist) SetOnClose(f func()) {
	s.onClose = f
}

func (s *Skiplist) getHeight() int32 {
	return s.height.Load()
}

func (s *Skiplist) randHeight() int {
	h := 1
	for h < maxHeight && z.FastRand() <= heightIncrease {
		h++
	}

	return h
}

// getNext => search from s.head.tower's offset to ... x.tower's offset =>
func (s *Skiplist) getNext(n *node, height int) *node {
	// search from tower to get next offset
	return s.arena.getNode(n.getNextOffset(height))
}

// findSpliceForLevel to find a key if it exactly matches the next return next, next
func (s *Skiplist) findSpliceForLevel(key []byte, before *node, level int) (*node, *node) {
	for {
		next := s.getNext(before, level)
		if next == nil {
			// before is the biggest node
			return before, next
		}
		nextKey := next.getKey(s.arena)
		cmp := utils.CompareKeys(key, nextKey) // To find a node happens to be greater than key
		if cmp == 0 {
			return next, next
		}
		if cmp < 0 {
			// before < key < next
			return before, next
		}
		// key > next
		before = next
	}
}

// findNear, less and allowEqual could be defined as
// [less: true, allowEqual: false => "<"], [less: true, allowEqual: true => "<="]
// [less: false, allowEqual: false => ">"], [less: true, allowEqual: true => ">="]
// Big-O: log(n) to search key on Skiplist
func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	// x is the search head, will move right if iterate over the same height
	x := s.head
	height := int(s.getHeight() - 1)
	for {
		next := s.getNext(x, height)
		if next == nil {
			// no node found in this height
			if height > 0 {
				// descend to lower height or iterate to the closer end
				height--
				continue
			}
			// base height
			if !less {
				return nil, false
			}
			// return x but make sure it's not head
			if x == s.head {
				return nil, false
			}
			return x, false
		}

		nextKey := next.getKey(s.arena)
		cmp := utils.CompareKeys(key, nextKey)
		if cmp > 0 {
			// key > nextKey, search on tower's order for a height
			x = next
			continue
		} else if cmp == 0 {
			if allowEqual {
				return next, true
			}
			if !less {
				// always found node in arena
				// so we could jump to base height to get the next node which is exactly greater than next
				return s.getNext(next, 0), false
			}
			if height > 0 {
				height--
				continue
			}
			// base height 0, return x but make sure it's not head
			if x == s.head {
				return nil, false
			}
			return x, false
		} else {
			// key < nextKey, we search from start in the lower height
			if height > 0 {
				height--
				continue
			}
			// base height
			if !less {
				return next, false
			}
			if x == s.head {
				return nil, false
			}
			return x, false
		}
	}
}

func (s *Skiplist) findLast() *node {
	x := s.head
	height := int(s.getHeight() - 1)
	for {
		next := s.getNext(x, height)
		if next != nil {
			// search on the height util reach to the end
			x = next
			continue
		}
		if x == s.head {
			return nil
		}
		if height == 0 {
			return x
		}
		height--
	}
}

// <---------------------- iterator --------------------->

// Iterator to iterate all the [key, value] pair on height 0
type Iterator struct {
	skl *Skiplist
	n   *node // The item to iterate over
}

func NewIterator(skl *Skiplist) *Iterator {
	skl.IncrRef()
	return &Iterator{skl: skl}
}

func (it *Iterator) Close() error {
	it.skl.DecrRef()
	return nil
}

func (it *Iterator) Valid() bool {
	return it.n != nil
}

func (it *Iterator) Next() {
	utils.AssertTrue(it.Valid())
	it.n = it.skl.getNext(it.n, 0)
}

func (it *Iterator) Prev() {
	utils.AssertTrue(it.Valid())
	key := it.n.getKey(it.skl.arena)
	it.n, _ = it.skl.findNear(key, true, false) // find "<"
}

func (it *Iterator) Seek(key []byte) {
	it.n, _ = it.skl.findNear(key, false, true) // ">=" key
}

func (it *Iterator) SeekToFirst() {
	it.n = it.skl.getNext(it.skl.head, 0)
}

func (it *Iterator) SeekToLast() {
	it.n = it.skl.findLast()
}

func (it *Iterator) Key() []byte {
	return it.n.getKey(it.skl.arena)
}

func (it *Iterator) Value() structs.ValueStruct {
	return it.n.getValue(it.skl.arena)
}
