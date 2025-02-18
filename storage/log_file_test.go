package storage

import (
	"bytes"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
	"tiny-badger/structs"
	"tiny-badger/utils"
)

var (
	logfileSize int64 = 1 << 20 // 1MB
)

func makeTmpFile() *os.File {
	f, _ := os.CreateTemp("/tmp", "logfile")
	return f
}

func destoryFile(path string) {
	err := os.Remove(path)
	if err != nil {
		panic(err)
	}
}

func TestOpen(t *testing.T) {
	f := makeTmpFile()
	defer destoryFile(f.Name())

	lf := newLogFile(f.Name(), int(f.Fd()))
	err := lf.open(os.O_RDWR, logfileSize)
	require.Equal(t, z.NewFile, err)
}

func TestEncodeDecode(t *testing.T) {
	lf := newLogFile("", 0)
	buf := new(bytes.Buffer)
	// empty entry
	entry := &structs.Entry{}
	sz, err := lf.encodeEntry(buf, entry, 0)
	require.NoError(t, err)
	require.EqualValues(t, 8, sz)
	e, err := lf.decodeEntry(buf.Bytes(), 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte{}, e.Key)
	require.EqualValues(t, []byte{}, e.Value)
	require.Equal(t, byte(0), e.Meta)
	require.Equal(t, uint64(0), e.ExpiresAt)

	buf.Reset()
	// normal value
	entry = &structs.Entry{
		Key:       []byte("key"),
		Value:     []byte("value"),
		Meta:      1,
		ExpiresAt: uint64(time.Now().Unix()),
	}
	sz, err = lf.encodeEntry(buf, entry, 0)
	require.NoError(t, err)
	require.EqualValues(t, 7+utils.SizeVarint(entry.ExpiresAt)+len(entry.Key)+len(entry.Value), sz)
	e, err = lf.decodeEntry(buf.Bytes(), 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte("key"), e.Key)
	require.EqualValues(t, []byte("value"), e.Value)
	require.Equal(t, byte(1), e.Meta)
	require.Equal(t, entry.ExpiresAt, e.ExpiresAt)
}

func TestWriteReadEntry(t *testing.T) {
	f := makeTmpFile()
	defer destoryFile(f.Name())

	lf := newLogFile(f.Name(), int(f.Fd()))
	err := lf.open(os.O_RDWR, logfileSize)
	require.Equal(t, z.NewFile, err)

	vp := structs.ValuePointer{
		Fid:    uint32(f.Fd()),
		Offset: lf.writeAt,
	}

	// write
	buf := new(bytes.Buffer)
	entry := &structs.Entry{
		Key:       []byte("key"),
		Value:     []byte("value"),
		Meta:      1,
		ExpiresAt: uint64(time.Now().Unix()),
	}
	err = lf.writeEntry(buf, entry)
	require.NoError(t, err)
	vp.Len = lf.writeAt - vp.Offset

	// read
	readBuf, err := lf.read(vp)
	require.NoError(t, err)
	e, err := lf.decodeEntry(readBuf, 0)
	require.NoError(t, err)
	require.EqualValues(t, []byte("key"), e.Key)
	require.EqualValues(t, []byte("value"), e.Value)
	require.Equal(t, byte(1), e.Meta)
	require.Equal(t, entry.ExpiresAt, e.ExpiresAt)
}
