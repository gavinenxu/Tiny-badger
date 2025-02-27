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

func TestDB_writeToLSM(t *testing.T) {
	dir := utils.CreateTmpDir("db-test")
	defer utils.DestroyDir(dir)

	opts := config.DefaultOptions(dir)
	opts.ReadOnly = false
	db, err := Open(opts)
	defer db.Close()
	require.NoError(t, err)
	require.NotNil(t, db)

	req := getRequest()
	defer req.Wg.Done()
	err = db.writeToLSM(req)
	require.NoError(t, err)

}

func TestDB_writeRequests(t *testing.T) {
	dir := utils.CreateTmpDir("db-test")
	defer utils.DestroyDir(dir)

	opts := config.DefaultOptions(dir)
	opts.ReadOnly = false
	db, err := Open(opts)
	defer db.Close()
	require.NoError(t, err)
	require.NotNil(t, db)

	reqs := make([]*request, 0, 10)
	for i := 0; i < 10; i++ {
		reqs = append(reqs, getRequest())
	}
	err = db.writeRequests(reqs)
	require.NoError(t, err)
}

func TestDB_sendToWriteCh(t *testing.T) {
	dir := utils.CreateTmpDir("db-test")
	defer utils.DestroyDir(dir)

	opts := config.DefaultOptions(dir)
	opts.ReadOnly = false
	db, err := Open(opts)
	defer db.Close()
	require.NoError(t, err)
	require.NotNil(t, db)

	var entries []*structs.Entry
	for i := 0; i < 10; i++ {
		entry := getEntry(i)
		entries = append(entries, entry)
	}

	for i := 0; i < 1000; i++ {
		err = db.sendToWriteCh(entries)
		require.NoError(t, err)
	}
}
