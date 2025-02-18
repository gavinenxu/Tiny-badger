package storage

import (
	"bytes"
	tiny_badger "tiny-badger"
	"tiny-badger/skl"
	"tiny-badger/structs"
	"tiny-badger/utils"
)

type MemTable struct {
	skl        *skl.Skiplist
	wal        *logFile
	opts       tiny_badger.Options
	buf        *bytes.Buffer
	maxVersion uint64 // max key's ts
}

func (mt *MemTable) Put(key []byte, value structs.ValueStruct) error {
	entry := &structs.Entry{
		Key:       key,
		Value:     value.Value,
		ExpiresAt: value.ExpiresAt,
		Meta:      value.Meta,
	}

	// in-memory don't need WAL
	if mt.wal != nil {
		if err := mt.wal.writeEntry(mt.buf, entry); err != nil {
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

func (mt *MemTable) isFull() bool {
	return false
}
