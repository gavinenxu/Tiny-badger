package skl

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"tiny-badger/structs"
	"tiny-badger/utils"
)

const arenaSize = 1 << 20 // 1MB

func newValue(val int) []byte {
	return []byte(fmt.Sprintf("%05d", val))
}

func length(s *Skiplist) int {
	count := 0
	n := s.getNext(s.head, 0)
	for n != nil {
		count++
		n = s.getNext(n, 0)
	}
	return count
}

func TestEmpty(t *testing.T) {
	key := []byte("key")
	l := NewSkiplist(arenaSize)

	v := l.Get(key)
	require.Nil(t, v.Value)

	for _, less := range []bool{false, true} {
		for _, allowEqual := range []bool{false, true} {
			n, eq := l.findNear(key, less, allowEqual)
			require.Nil(t, n)
			require.False(t, eq)
		}
	}
}

func TestBasic(t *testing.T) {
	l := NewSkiplist(arenaSize)
	v1 := newValue(10)
	v2 := newValue(20)
	v3 := newValue(30)
	v4 := newValue(40)
	v5 := newValue(50)

	l.Put(utils.KeyWithTs([]byte("key1"), 0), structs.ValueStruct{Value: v1, Meta: 55})
	l.Put(utils.KeyWithTs([]byte("key2"), 2), structs.ValueStruct{Value: v2, Meta: 56})
	l.Put(utils.KeyWithTs([]byte("key3"), 0), structs.ValueStruct{Value: v3, Meta: 57})

	v := l.Get(utils.KeyWithTs([]byte("key"), 0))
	require.True(t, v.Value == nil)

	v = l.Get(utils.KeyWithTs([]byte("key1"), 0))
	require.True(t, v.Value != nil)
	require.Equal(t, v1, v.Value)
	require.Equal(t, byte(55), v.Meta)

	v = l.Get(utils.KeyWithTs([]byte("key2"), 0))
	require.True(t, v.Value == nil)

	v = l.Get(utils.KeyWithTs([]byte("key2"), 2))
	require.True(t, v.Value != nil)
	require.Equal(t, v2, v.Value)
	require.Equal(t, byte(56), v.Meta)

	v = l.Get(utils.KeyWithTs([]byte("key3"), 0))
	require.True(t, v.Value != nil)
	require.Equal(t, v3, v.Value)
	require.Equal(t, byte(57), v.Meta)

	// put new value for a key with new ts
	l.Put(utils.KeyWithTs([]byte("key3"), 1), structs.ValueStruct{Value: v4, Meta: 100})
	v = l.Get(utils.KeyWithTs([]byte("key3"), 1))
	require.True(t, v.Value != nil)
	require.Equal(t, v4, v.Value)
	require.Equal(t, byte(100), v.Meta)

	l.Put(utils.KeyWithTs([]byte("key4"), 1), structs.ValueStruct{Value: v5, Meta: 200})
	v = l.Get(utils.KeyWithTs([]byte("key4"), 1))
	require.True(t, v.Value != nil)
	require.Equal(t, v5, v.Value)
	require.Equal(t, byte(200), v.Meta)
}

func TestFindNearest(t *testing.T) {
	l := NewSkiplist(arenaSize)

	for i := 1000 - 1; i >= 0; i-- {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put(utils.KeyWithTs([]byte(key), 0), structs.ValueStruct{Value: newValue(i)})
	}

	// case 1
	n, eq := l.findNear(utils.KeyWithTs([]byte("00001"), 0), false, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00005"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("00001"), 0), false, true)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00005"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("00001"), 0), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(utils.KeyWithTs([]byte("00001"), 0), true, true)
	require.Nil(t, n)
	require.False(t, eq)

	// case 2
	n, eq = l.findNear(utils.KeyWithTs([]byte("00005"), 0), false, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00015"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("00005"), 0), false, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00005"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("00005"), 0), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(utils.KeyWithTs([]byte("00005"), 0), true, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00005"), 0), n.key(l.arena))

	// case 3
	n, eq = l.findNear(utils.KeyWithTs([]byte("05555"), 0), false, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05565"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05555"), 0), false, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05555"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05555"), 0), true, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05545"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05555"), 0), true, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05555"), 0), n.key(l.arena))

	// case 3
	n, eq = l.findNear(utils.KeyWithTs([]byte("05558"), 0), false, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05565"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05558"), 0), false, true)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05565"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05558"), 0), true, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05555"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05558"), 0), true, true)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05555"), 0), n.key(l.arena))

	// case 4 largest key
	n, eq = l.findNear(utils.KeyWithTs([]byte("09995"), 0), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(utils.KeyWithTs([]byte("09995"), 0), false, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09995"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("09995"), 0), true, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09985"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("09995"), 0), true, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09995"), 0), n.key(l.arena))

	// case 5 beyond largest key
	n, eq = l.findNear(utils.KeyWithTs([]byte("59995"), 0), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(utils.KeyWithTs([]byte("59995"), 0), false, true)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(utils.KeyWithTs([]byte("59995"), 0), true, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09995"), 0), n.key(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("59995"), 0), true, true)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09995"), 0), n.key(l.arena))
}

func TestConcurrentBasic(t *testing.T) {
	var wg sync.WaitGroup

	l := NewSkiplist(arenaSize)
	n := 1000

	key := func(i int) []byte {
		return utils.KeyWithTs([]byte(fmt.Sprintf("%05d", i)), 0)
	}
	// put value
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Put(key(i), structs.ValueStruct{Value: newValue(i), Meta: 0})
		}()
	}
	wg.Wait()

	// read value
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := l.Get(key(i))
			require.NotNil(t, v)
			require.Equal(t, newValue(i), v.Value)
			require.Equal(t, byte(0), v.Meta)
		}()
	}
	wg.Wait()

	require.Equal(t, n, length(l))
}

func TestConcurrentBasicBigValue(t *testing.T) {
	var wg sync.WaitGroup

	l := NewSkiplist(120 << 20) // 120MB
	n := 100

	key := func(i int) []byte {
		return utils.KeyWithTs([]byte(fmt.Sprintf("%05d", i)), 0)
	}
	bigValue := func(val int) []byte {
		return []byte(fmt.Sprintf("%01048576d", val))
	}
	// put value
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Put(key(i), structs.ValueStruct{Value: bigValue(i), Meta: 0})
		}()
	}
	wg.Wait()

	// read value
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := l.Get(key(i))
			require.NotNil(t, v)
			require.Equal(t, bigValue(i), v.Value)
			require.Equal(t, byte(0), v.Meta)
		}()
	}
	wg.Wait()

	require.Equal(t, n, length(l))
}

func TestOneKey(t *testing.T) {
	var wg sync.WaitGroup
	l := NewSkiplist(arenaSize)
	n := 100

	key := utils.KeyWithTs([]byte(fmt.Sprintf("onekey")), 0)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Put(key, structs.ValueStruct{Value: newValue(i), Meta: 0})
		}()
	}

	// we expect to read one value
	var writtenValues atomic.Uint32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := l.Get(key)
			if v.Value == nil {
				return
			}
			writtenValues.Add(1)
			require.Equal(t, byte(0), v.Meta)
			vv, err := strconv.Atoi(string(v.Value))
			require.NoError(t, err)
			require.True(t, vv >= 0 && vv < n)
		}()
	}

	wg.Wait()

	require.True(t, writtenValues.Load() > 0)
	require.Equal(t, 1, length(l))
}

func randomKey(r *rand.Rand) []byte {
	key := make([]byte, 8)
	k1, k2 := r.Uint32(), r.Uint32()
	binary.BigEndian.PutUint32(key[:4], k1)
	binary.BigEndian.PutUint32(key[4:], k2)
	return utils.KeyWithTs(key, 0)
}

func BenchmarkPut(b *testing.B) {
	value := newValue(100)
	l := NewSkiplist(int64((b.N + 1) * MaxNodeSize))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			l.Put(randomKey(r), structs.ValueStruct{Value: value, Meta: 0})
		}
	})
}
