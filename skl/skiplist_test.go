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
	defer l.DecrRef()

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
	defer l.DecrRef()
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
	defer l.DecrRef()

	for i := 1000 - 1; i >= 0; i-- {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put(utils.KeyWithTs([]byte(key), 0), structs.ValueStruct{Value: newValue(i)})
	}

	// case 1
	n, eq := l.findNear(utils.KeyWithTs([]byte("00001"), 0), false, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00005"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("00001"), 0), false, true)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00005"), 0), n.getKey(l.arena))
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
	require.EqualValues(t, utils.KeyWithTs([]byte("00015"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("00005"), 0), false, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00005"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("00005"), 0), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(utils.KeyWithTs([]byte("00005"), 0), true, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("00005"), 0), n.getKey(l.arena))

	// case 3
	n, eq = l.findNear(utils.KeyWithTs([]byte("05555"), 0), false, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05565"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05555"), 0), false, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05555"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05555"), 0), true, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05545"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05555"), 0), true, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05555"), 0), n.getKey(l.arena))

	// case 3
	n, eq = l.findNear(utils.KeyWithTs([]byte("05558"), 0), false, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05565"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05558"), 0), false, true)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05565"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05558"), 0), true, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05555"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("05558"), 0), true, true)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("05555"), 0), n.getKey(l.arena))

	// case 4 largest key
	n, eq = l.findNear(utils.KeyWithTs([]byte("09995"), 0), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear(utils.KeyWithTs([]byte("09995"), 0), false, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09995"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("09995"), 0), true, false)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09985"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("09995"), 0), true, true)
	require.NotNil(t, n)
	require.True(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09995"), 0), n.getKey(l.arena))

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
	require.EqualValues(t, utils.KeyWithTs([]byte("09995"), 0), n.getKey(l.arena))
	n, eq = l.findNear(utils.KeyWithTs([]byte("59995"), 0), true, true)
	require.NotNil(t, n)
	require.False(t, eq)
	require.EqualValues(t, utils.KeyWithTs([]byte("09995"), 0), n.getKey(l.arena))
}

func TestConcurrentBasic(t *testing.T) {
	var wg sync.WaitGroup

	l := NewSkiplist(arenaSize)
	defer l.DecrRef()
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
	defer l.DecrRef()
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
	defer l.DecrRef()
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

func TestIteratorNext(t *testing.T) {
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()
	iter := NewIterator(l)
	defer iter.Close()
	require.False(t, iter.Valid())
	iter.SeekToFirst()
	require.False(t, iter.Valid())

	n := 100
	for i := n - 1; i >= 0; i-- {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put(utils.KeyWithTs([]byte(key), 0), structs.ValueStruct{Value: newValue(i)})
	}

	iter.SeekToFirst()
	require.True(t, iter.Valid())
	for i := 0; iter.Valid(); iter.Next() {
		require.True(t, iter.Valid())
		key := fmt.Sprintf("%05d", i*10+5)
		require.Equal(t, utils.KeyWithTs([]byte(key), 0), iter.Key())

		value := iter.Value()
		require.Equal(t, newValue(i), value.Value)

		i++
	}
}

func TestIteratorPrev(t *testing.T) {
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()
	iter := NewIterator(l)
	defer iter.Close()
	require.False(t, iter.Valid())
	iter.SeekToLast()
	require.False(t, iter.Valid())

	n := 100
	for i := n - 1; i >= 0; i-- {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put(utils.KeyWithTs([]byte(key), 0), structs.ValueStruct{Value: newValue(i)})
	}

	iter.SeekToLast()
	require.True(t, iter.Valid())
	for i := n - 1; iter.Valid(); iter.Prev() {
		require.True(t, iter.Valid())
		key := fmt.Sprintf("%05d", i*10+5)
		require.Equal(t, utils.KeyWithTs([]byte(key), 0), iter.Key())

		value := iter.Value()
		require.Equal(t, newValue(i), value.Value)

		i--
	}
}

func TestIteratorSeek(t *testing.T) {
	l := NewSkiplist(arenaSize)
	defer l.DecrRef()
	iter := NewIterator(l)
	defer iter.Close()
	require.False(t, iter.Valid())
	iter.SeekToFirst()
	require.False(t, iter.Valid())

	n := 100
	for i := n - 1; i >= 0; i-- {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put(utils.KeyWithTs([]byte(key), 0), structs.ValueStruct{Value: newValue(i)})
	}

	iter.SeekToFirst()
	require.True(t, iter.Valid())
	require.Equal(t, utils.KeyWithTs([]byte("00005"), 0), iter.Key())
	require.EqualValues(t, "00000", iter.Value().Value)

	iter.Seek(utils.KeyWithTs([]byte("00010"), 0))
	require.True(t, iter.Valid())
	require.Equal(t, utils.KeyWithTs([]byte("00015"), 0), iter.Key())
	require.EqualValues(t, "00001", string(iter.Value().Value))

	iter.Seek(utils.KeyWithTs([]byte("00025"), 0))
	require.True(t, iter.Valid())
	require.Equal(t, utils.KeyWithTs([]byte("00025"), 0), iter.Key())
	require.EqualValues(t, "00002", string(iter.Value().Value))

	iter.Seek(utils.KeyWithTs([]byte("00995"), 0))
	require.True(t, iter.Valid())
	require.Equal(t, utils.KeyWithTs([]byte("00995"), 0), iter.Key())
	require.EqualValues(t, "00099", string(iter.Value().Value))

	iter.Seek(utils.KeyWithTs([]byte("01000"), 0))
	require.False(t, iter.Valid())
}

func randomKey(r *rand.Rand) []byte {
	key := make([]byte, 8)
	k1, k2 := r.Uint32(), r.Uint32()
	binary.BigEndian.PutUint32(key[:4], k1)
	binary.BigEndian.PutUint32(key[4:], k2)
	return utils.KeyWithTs(key, 0)
}

func BenchmarkPut(b *testing.B) {
	value := newValue(123)
	l := NewSkiplist(int64((b.N + 1) * MaxNodeSize))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			l.Put(randomKey(r), structs.ValueStruct{Value: value, Meta: 0})
		}
	})
}

// Some fraction is read. Some fraction is write.
func BenchmarkGetPut(b *testing.B) {
	value := newValue(123)
	for i := 0; i < 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := NewSkiplist(int64((b.N + 1) * MaxNodeSize))
			defer l.DecrRef()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if r.Float32() < readFrac {
						l.Get(randomKey(r))
					} else {
						l.Put(randomKey(r), structs.ValueStruct{Value: value, Meta: 0})
					}
				}
			})
		})
	}
}

func BenchmarkGetWriteMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i < 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if r.Float32() < readFrac {
						mutex.RLock()
						_ = m[string(randomKey(r))]
						mutex.RUnlock()
					} else {
						mutex.Lock()
						m[string(randomKey(r))] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}
