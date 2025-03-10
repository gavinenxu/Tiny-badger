package tiny_badger

import (
	"tiny-badger/structs"
	"tiny-badger/utils"
)

// oracle The timestamp manager and conflict detector for transactions
type oracle struct {
}

type Item struct {
	key       []byte
	vptr      []byte
	value     []byte
	version   uint64
	expiresAt uint64

	txn *Txn
}

type Txn struct {
	pendingWrites map[string]*structs.Entry // cache writes during transaction

	readTs   uint64
	commitTs uint64
	db       *DB

	discarded bool
}

func (db *DB) NewTransaction() *Txn {
	txn := &Txn{
		db:            db,
		pendingWrites: make(map[string]*structs.Entry),
	}
	return txn
}

func (txn *Txn) Set(key, value []byte) error {
	return txn.SetEntry(structs.NewEntry(key, value))
}

func (txn *Txn) Get(key []byte) (*Item, error) {
	if len(key) == 0 {
		return nil, utils.ErrEmptyKey
	} else if txn.discarded {
		return nil, utils.ErrDiscardedTxn
	}

	item := new(Item)

	// todo set update to get value from pendingWrites

	seek := utils.KeyWithTs(key, txn.readTs)
	vs, err := txn.db.get(seek)
	if err != nil {
		return nil, utils.Wrapf(err, "DB::Get key: %q", key)
	}
	if vs.Value == nil && vs.Meta == 0 {
		return nil, utils.ErrKeyNotFound
	}
	if utils.IsDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return nil, utils.ErrKeyNotFound
	}

	item.key = key
	item.vptr = utils.SafeCopy(item.vptr, vs.Value)
	item.version = vs.Version
	item.expiresAt = vs.ExpiresAt
	item.txn = txn
	return item, nil
}

func (txn *Txn) SetEntry(entry *structs.Entry) error {
	return txn.modify(entry)
}

func (txn *Txn) Commit() error {
	if len(txn.pendingWrites) == 0 {
		txn.Discard()
		return nil
	}

	defer txn.Discard()

	commitCb, err := txn.commitAndSend()
	if err != nil {
		return err
	}
	return commitCb()
}

func (txn *Txn) Discard() {
	if txn.discarded {
		return
	}
	txn.discarded = true
}

// modify The internal methods to change the db value
func (txn *Txn) modify(entry *structs.Entry) error {
	txn.pendingWrites[string(entry.Key)] = entry
	return nil
}

// commitAndSend internal method to send db changes to write channel
func (txn *Txn) commitAndSend() (func() error, error) {
	entries := make([]*structs.Entry, 0, len(txn.pendingWrites))
	for _, entry := range txn.pendingWrites {
		entries = append(entries, entry)
	}
	req, err := txn.db.sendToWriteCh(entries)
	if err != nil {
		return nil, err
	}
	ret := func() error {
		// wait request to finish
		err = req.Wait()
		return err
	}
	return ret, err
}
