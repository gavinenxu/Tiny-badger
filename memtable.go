package tiny_badger

import (
	"bytes"
	"fmt"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"tiny-badger/config"
	"tiny-badger/skl"
	"tiny-badger/storage"
	"tiny-badger/structs"
	"tiny-badger/utils"
)

// MemTable structure stores a skiplist and a corresponding WAL. Writes to memTable are written
// both to the WAL and the skiplist. On a crash, the WAL is replayed to bring the skiplist back to
// its pre-crash form.
type MemTable struct {
	skl        *skl.Skiplist
	wal        *storage.LogFile
	opts       config.Options
	buf        *bytes.Buffer
	maxVersion uint64 // max key's ts
}

const memFileExt = ".mem"

// openMemTables open memtables from db.dir and assign it to db.imm
func (db *DB) openMemTables() error {
	if db.opts.InMemory {
		return nil
	}
	files, err := os.ReadDir(db.opts.Dir)
	if err != nil {
		return utils.Wrapf(err, "open dir %s for memtable", db.opts.Dir)
	}

	var fids []int
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), memFileExt) {
			continue
		}
		sz := len(file.Name())
		fid, err := strconv.ParseInt(file.Name()[:sz-len(memFileExt)], 10, 64)
		if err != nil {
			return utils.Wrapf(err, "parse file %s to int", file.Name())
		}
		fids = append(fids, int(fid))
	}

	sort.Ints(fids)
	for _, fid := range fids {
		flags := os.O_RDWR
		if db.opts.ReadOnly {
			flags = os.O_RDONLY
		}
		mt, err := db.openMemTable(fid, flags)
		if err != nil {
			return utils.Wrapf(err, "open memtable for fid %d", fid)
		}
		// If this memtable is truncated, we don't need to add it
		if mt.skl.IsEmpty() {
			mt.DecrRef()
			continue
		}
		db.imm = append(db.imm, mt)
	}

	if len(fids) != 0 {
		db.nextMemFid = fids[len(fids)-1]
	}
	db.nextMemFid++

	return nil
}

// openMemTable from an existing file id with flags
func (db *DB) openMemTable(fid int, flags int) (*MemTable, error) {
	s := skl.NewSkiplist(db.arenaSize())
	mt := &MemTable{
		skl:  s,
		opts: db.opts,
		buf:  new(bytes.Buffer),
	}
	if db.opts.InMemory {
		// don't need to create wal for in-memory mode
		return mt, z.NewFile
	}

	path := mtFilePath(db.opts.Dir, fid)
	mt.wal = storage.NewLogFile(path, fid)
	err := mt.wal.Open(flags, 2*db.opts.MemtableSize)
	if err != z.NewFile && err != nil {
		return nil, utils.Wrapf(err, "while opening memtable for path %s", path)
	}
	s.SetOnClose(func() {
		// skiplist ref decrease to 0, to remove the wal
		if err := mt.wal.Delete(); err != nil {
			db.log.Errorf("while deleting memtable for path %s, error: %v", path, err)
		}
	})
	if err == z.NewFile {
		return mt, err
	}

	// todo update skiplist

	return mt, nil
}

func (db *DB) newMemTable() (*MemTable, error) {
	// set create mode
	mt, err := db.openMemTable(db.nextMemFid, os.O_RDWR|os.O_CREATE)
	if err == z.NewFile {
		db.nextMemFid++
		return mt, nil
	}

	if err != nil {
		db.log.Errorf("while open memtable for fid %d, error: %v", db.nextMemFid, err)
		return nil, utils.Wrapf(err, "newMemtable")
	}
	// no error while open file, return file exist error
	return nil, errors.Errorf("File %s already exists", mt.wal.Fd.Name())
}

func (mt *MemTable) Put(key []byte, value structs.ValueStruct) error {
	entry := &structs.Entry{
		Key:       key,
		Value:     value.Value,
		ExpiresAt: value.ExpiresAt,
		Meta:      value.Meta,
		UserMeta:  value.UserMeta,
	}

	// in-memory don't need WAL
	if mt.wal != nil {
		if err := mt.wal.WriteEntry(mt.buf, entry); err != nil {
			return utils.Wrapf(err, "cannot write entry to WAL file")
		}
	}

	// Write to skiplist
	mt.skl.Put(key, value)
	if ts := utils.ParseTs(key); ts > mt.maxVersion {
		mt.maxVersion = ts
	}
	return nil
}

func (mt *MemTable) IncrRef() {
	mt.skl.IncrRef()
}

func (mt *MemTable) DecrRef() {
	mt.skl.DecrRef()
}

func (mt *MemTable) SyncWal() error {
	return mt.wal.Sync()
}

func (mt *MemTable) isFull() bool {
	return false
}

func mtFilePath(dirname string, fid int) string {
	return filepath.Join(dirname, fmt.Sprintf("%05d%s", fid, memFileExt))
}
