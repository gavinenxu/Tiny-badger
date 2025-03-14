package tiny_badger

import (
	"expvar"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/pkg/errors"
	"sync"
	"sync/atomic"
	"tiny-badger/config"
	"tiny-badger/structs"
	"tiny-badger/utils"
)

const (
	kvWriteChCapacity = 1000
)

var requestPool = sync.Pool{
	New: func() interface{} {
		return new(request)
	},
}

type closers struct {
	writes *z.Closer
}

type DB struct {
	writeCh chan *request
	opts    config.Options
	log     utils.Logger

	mt         *MemTable   // current active memtable
	imm        []*MemTable // immutable memtables
	nextMemFid int

	isClosed atomic.Uint32

	lock sync.RWMutex // guards list of inmemory tables

	closers closers
}

func Open(opts config.Options) (*DB, error) {
	db := &DB{
		writeCh: make(chan *request, kvWriteChCapacity),
		imm:     make([]*MemTable, 0),
		opts:    opts,
		log:     utils.NewDefaultLogger(utils.ERROR),
	}
	var err error

	if err := db.openMemTables(); err != nil {
		return nil, utils.Wrapf(err, "while open memtables")
	}
	if !db.opts.ReadOnly {
		if db.mt, err = db.newMemTable(); err != nil {
			return nil, utils.Wrapf(err, "create new memtable")
		}
	}

	db.closers.writes = z.NewCloser(1)
	go db.doWrites(db.closers.writes)

	return db, nil
}

func (db *DB) Close() error {
	// send HasBeenClosed signal, gracefully close writes
	db.closers.writes.SignalAndWait()
	close(db.writeCh)

	if db.mt != nil {
		if db.mt.skl.IsEmpty() {
			// remove memtable
			db.mt.DecrRef()
		} else {
			// todo flush memtable
		}
	}

	return nil
}

func (db *DB) sendToWriteCh(entries []*structs.Entry) (*request, error) {
	// todo calc metrics and determine whether to execute next request

	req := requestPool.Get().(*request)
	req.reset()
	req.Entries = entries
	req.Wg.Add(1)
	db.writeCh <- req // handled in doWrites

	return req, nil
}

// doWrites handles concurrent writes to memtable in sequential order in a batch
func (db *DB) doWrites(lc *z.Closer) {
	defer lc.Done()
	pendingCh := make(chan struct{}, 1)

	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			db.log.Errorf("writeRequests: %v", err)
		}
		<-pendingCh
	}

	// This variable tracks the number of pending writes.
	reqLen := new(expvar.Int)

	reqs := make([]*request, 0, 10)
	for {
		var r *request
		select {
		case r = <-db.writeCh:
		case <-lc.HasBeenClosed():
			goto closedCase
		}

		for {
			reqs = append(reqs, r)
			reqLen.Set(int64(len(reqs)))

			if len(reqs) >= 3*kvWriteChCapacity {
				pendingCh <- struct{}{} // blocking.
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			case r = <-db.writeCh:
			case pendingCh <- struct{}{}:
				goto writeCase
			case <-lc.HasBeenClosed():
				goto closedCase
			}
		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		for {
			select {
			case r = <-db.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing a write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
		reqLen.Set(0)
	}
}

//func (db *DB) doWrites(lc *z.Closer) {
//	defer lc.Done()
//	// handle batch write
//	pendingCh := make(chan struct{}, 1)
//
//	writeRequestsBatch := func(reqs []*request) {
//		if err := db.writeRequests(reqs); err != nil {
//			db.log.Errorf("writeRequests: %v", err)
//		}
//		<-pendingCh
//	}
//
//	reqs := make([]*request, 0, 10)
//	for {
//		var r *request
//		select {
//		case r = <-db.writeCh:
//		case <-lc.HasBeenClosed():
//			goto closeCase
//		}
//
//		for {
//			reqs = append(reqs, r)
//
//			// force to flush to disk
//			if len(reqs) >= 3*kvWriteChCapacity {
//				pendingCh <- struct{}{} // blocking until previous batch finish
//				goto writeCase
//			}
//
//			// choose the case randomly, either to pick from write channel or write to db and set to pending state
//			select {
//			case r = <-db.writeCh: // continue to pick from writeCh
//			case pendingCh <- struct{}{}: // push to pending chan and execute this batch write
//				goto writeCase
//			case <-lc.HasBeenClosed():
//				goto closeCase
//			}
//		}
//
//	writeCase:
//		go writeRequestsBatch(reqs)
//		// reset new batch of requests
//		reqs = make([]*request, 0, 10)
//
//	closeCase:
//		for {
//			select {
//			// drain the pending requests
//			case r = <-db.writeCh:
//				reqs = append(reqs, r)
//			default:
//				pendingCh <- struct{}{}
//				writeRequestsBatch(reqs)
//				return
//			}
//		}
//	}
//}

// writeRequests is called serially by only one goroutine.
func (db *DB) writeRequests(reqs []*request) error {
	if len(reqs) == 0 {
		return nil
	}

	done := func(err error) {
		for _, req := range reqs {
			req.Err = err
			req.Wg.Done()
		}
	}

	// 1, write to value log
	db.log.Debugf("writeRequests called. Writing to value log... (not suppported now)")

	// 2. write to memtable
	db.log.Debugf("Writing to memtable")
	var count int
	for _, req := range reqs {
		if len(req.Entries) == 0 {
			continue
		}
		count++

		if err := db.writeToLSM(req); err != nil {
			done(err)
			return errors.Wrap(err, "writeRequests")
		}
	}

	done(nil)
	db.log.Debugf("%d entries written", count)
	return nil
}

func (db *DB) writeToLSM(req *request) error {
	for _, entry := range req.Entries {
		// todo set threshold if value is too large, and write the value pointer to memtable
		if err := db.mt.Put(entry.Key, structs.ValueStruct{
			Value:     entry.Value,
			ExpiresAt: entry.ExpiresAt,
			Meta:      entry.Meta, // todo set bitValuePointer??
			UserMeta:  entry.UserMeta,
		}); err != nil {
			return utils.Wrapf(err, "while writing to memTable")
		}
	}
	if db.opts.SyncWrites {
		return db.mt.SyncWal()
	}
	return nil
}

func (db *DB) IsClosed() bool {
	return db.isClosed.Load() == 1
}

func (db *DB) arenaSize() int64 {
	// todo calc the correct arena size
	return db.opts.MemtableSize
}

func (db *DB) get(key []byte) (structs.ValueStruct, error) {
	if db.IsClosed() {
		return structs.ValueStruct{}, utils.ErrDBClosed
	}

	tables, decrFn := db.getMemtables()
	defer decrFn()

	var maxVs structs.ValueStruct
	version := utils.ParseTs(key)

	for _, table := range tables {
		vs := table.skl.Get(key)
		if vs.Meta == 0 && vs.Value == nil {
			continue
		}
		// found the value from latest table
		if vs.Version == version {
			return vs, nil
		}
		if maxVs.Version < vs.Version {
			maxVs = vs
		}
	}

	return maxVs, nil
	// todo get from level controller
	//return structs.ValueStruct{}, nil
}

// getMemtables from latest records to the oldest records
func (db *DB) getMemtables() ([]*MemTable, func()) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var tables []*MemTable

	if !db.opts.ReadOnly {
		// mutable table
		tables = append(tables, db.mt)
		db.mt.IncrRef()
	}

	lastIdx := len(db.imm) - 1
	for i := range db.imm {
		// immutable tables
		tables = append(tables, db.imm[lastIdx-i])
		db.imm[lastIdx-i].IncrRef()
	}

	return tables, func() {
		for _, table := range tables {
			table.DecrRef()
		}
	}
}
