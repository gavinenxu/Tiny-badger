package tiny_badger

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"tiny-badger/config"
	"tiny-badger/structs"
	"tiny-badger/utils"
)

func getEntry(i int) *structs.Entry {
	return &structs.Entry{
		Key:       utils.KeyWithTs([]byte(fmt.Sprintf("%05d", i*10+5)), 0),
		Value:     newValue(i),
		ExpiresAt: uint64(i),
		Meta:      byte(i),
		UserMeta:  byte(i + 1),
	}
}

func getRequest() *request {
	req := requestPool.Get().(*request)
	for i := 0; i < 100; i++ {
		entry := getEntry(i)
		req.Entries = append(req.Entries, entry)
	}
	req.Wg.Add(1)
	return req
}

func getItemValue(t *testing.T, item *Item) []byte {
	return item.vptr
}

func txnSet(t *testing.T, kv *DB, key []byte, value []byte, meta byte) {
	txn := kv.NewTransaction()
	require.NoError(t, txn.SetEntry(structs.NewEntry(key, value).WithMeta(meta)))
	require.NoError(t, txn.Commit())
}

func runBadgerTest(t *testing.T, opts *config.Options, test func(t *testing.T, db *DB)) {
	dir := utils.CreateTmpDir("badger-test")
	defer utils.DestroyDir(dir)

	if opts == nil {
		opts = new(config.Options)
		*opts = config.DefaultOptions(dir)
	} else {
		opts.Dir = dir
	}

	db, err := Open(*opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()
	test(t, db)
}

func TestDB_writeToLSM(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		req := getRequest()
		defer req.Wg.Done()
		err := db.writeToLSM(req)
		require.NoError(t, err)
	})
}

func TestDB_writeRequests(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		reqs := make([]*request, 0, 10)
		for i := 0; i < 10; i++ {
			reqs = append(reqs, getRequest())
		}
		err := db.writeRequests(reqs)
		require.NoError(t, err)
	})
}

func TestDB_sendToWriteCh(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		var entries []*structs.Entry
		for i := 0; i < 10; i++ {
			entry := getEntry(i)
			entries = append(entries, entry)
		}

		for i := 0; i < 1000; i++ {
			_, err := db.sendToWriteCh(entries)
			require.NoError(t, err)
		}
	})
}

func TestWrite(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		for i := 0; i < 20; i++ {
			txnSet(t, db, utils.KeyWithTs([]byte(fmt.Sprintf("key%d", i)), 0), []byte(fmt.Sprintf("val%d", i)), 0x00)
		}
	})
}

func TestGet(t *testing.T) {
	runBadgerTest(t, nil, func(t *testing.T, db *DB) {
		txnSet(t, db, utils.KeyWithTs([]byte("key1"), 0), []byte("val1"), 0x08)

		txn := db.NewTransaction()
		item, err := txn.Get([]byte("key1"))
		require.NoError(t, err)
		require.Equal(t, []byte("val1"), getItemValue(t, item))
		txn.Discard()
	})
}
