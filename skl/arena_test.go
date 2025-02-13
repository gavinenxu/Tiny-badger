package skl

import (
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"tiny-badger/structs"
)

func TestArenaBasic(t *testing.T) {
	a := newArena(math.MaxUint32)
	offset := a.putNode(1)
	require.Equal(t, uint32(8), offset)
	n := a.getNode(offset)
	require.NotNil(t, n)

	sz := a.size()
	offset = a.putKey([]byte{1, 2, 3})
	require.Equal(t, uint32(sz), offset)
	key := a.getKey(offset, 3)
	require.Equal(t, []byte{1, 2, 3}, key)

	v := structs.ValueStruct{
		Meta:      1,
		ExpiresAt: 1,
		Value:     []byte{1, 2, 3},
	}
	sz = a.size()
	offset = a.putValue(v)
	require.Equal(t, uint32(sz), offset)
	val := a.getValue(offset, v.EncodedSize())
	require.Equal(t, byte(1), val.Meta)
	require.Equal(t, uint64(1), val.ExpiresAt)
	require.Equal(t, []byte{1, 2, 3}, val.Value)

	require.Equal(t, a.size(), int64(offset+v.EncodedSize()))
}
