package storage

import (
	"bytes"
	"encoding/binary"
	"github.com/dgraph-io/ristretto/v2/z"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	tinyBadger "tiny-badger"
	"tiny-badger/structs"
	"tiny-badger/utils"
)

const (
	// size of vlog header
	// +----------------+------------------+
	// | keyID(8 bytes) | baseIV(12 bytes) |
	// +----------------+------------------+
	vlogHeaderSize = 20
)

// logFile inheritance mmap
type logFile struct {
	*z.MmapFile
	path    string
	lock    sync.RWMutex
	fid     uint32
	size    atomic.Uint32
	writeAt uint32
	opts    tinyBadger.Options
}

func newLogFile(path string, fid int) *logFile {
	return &logFile{
		fid:     uint32(fid),
		path:    path,
		writeAt: vlogHeaderSize,
	}
}

func (lf *logFile) open(flags int, fsize int64) error {
	mf, err := z.OpenMmapFile(lf.path, flags, int(fsize))
	lf.MmapFile = mf

	if err == z.NewFile {

	} else if err != nil {
		return utils.Wrapf(err, "while opening file: %s", lf.path)
	}

	return err
}

func (lf *logFile) writeEntry(buf *bytes.Buffer, entry *structs.Entry) error {
	buf.Reset()
	recordLen, err := lf.encodeEntry(buf, entry, lf.writeAt)
	if err != nil {
		return err
	}
	// write data to file using mmap
	utils.AssertTrue(recordLen == copy(lf.Data[lf.writeAt:], buf.Bytes()))
	lf.writeAt += uint32(recordLen)
	return nil
}

func (lf *logFile) read(p structs.ValuePointer) (buf []byte, err error) {
	size := int64(len(lf.Data))
	if int64(p.Offset) >= size || int64(p.Offset+p.Len) > size {
		err = utils.ErrEOF
	} else {
		buf = lf.Data[p.Offset : p.Offset+p.Len]
	}
	return buf, err
}

// encodeEntry will encode entry to the buf
// layout of entry
// +--------+-----+-------+-------+
// | header | key | value | crc32 |
// +--------+-----+-------+-------+
func (lf *logFile) encodeEntry(buf *bytes.Buffer, entry *structs.Entry, offset uint32) (int, error) {
	h := structs.Header{
		KeyLen:    uint32(len(entry.Key)),
		ValLen:    uint32(len(entry.Value)),
		ExpiresAt: entry.ExpiresAt,
		Meta:      entry.Meta,
	}

	// 1. create io writer
	hash := crc32.New(utils.CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	// 2. encode header
	var headerEnc [structs.MaxHeaderSize]byte
	sz := h.Encode(headerEnc[:])

	// 3. write header, key, value
	utils.Check2(writer.Write(headerEnc[:sz]))
	utils.Check2(writer.Write(entry.Key))
	utils.Check2(writer.Write(entry.Value))

	// 4. compute crc and write to buf
	var crcBuf [crc32.Size]byte
	binary.LittleEndian.PutUint32(crcBuf[:], hash.Sum32())
	utils.Check2(writer.Write(crcBuf[:]))

	return len(headerEnc[:sz]) + len(entry.Key) + len(entry.Value) + len(crcBuf), nil
}

func (lf *logFile) decodeEntry(buf []byte, offset uint32) (*structs.Entry, error) {
	var h structs.Header
	headerLen := h.Decode(buf)
	kv := buf[headerLen:]
	e := &structs.Entry{
		Key:       kv[:h.KeyLen],
		Value:     kv[h.KeyLen : h.KeyLen+h.ValLen],
		ExpiresAt: h.ExpiresAt,
		Meta:      h.Meta,
	}
	return e, nil
}
