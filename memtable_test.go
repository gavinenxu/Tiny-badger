package tiny_badger

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"tiny-badger/config"
	"tiny-badger/structs"
	"tiny-badger/utils"
)

func newValue(val int) []byte {
	return []byte(fmt.Sprintf("%05d", val))
}

func TestNewMemtable(t *testing.T) {
	dir := utils.CreateTmpDir("memtable-test")
	defer utils.DestroyDir(dir)

	opts := config.DefaultOptions(dir)
	db, err := Open(opts)
	defer db.Close()
	require.NoError(t, err)
	require.NotNil(t, db)

	mt, err := db.newMemTable()
	require.NoError(t, err)
	require.NotNil(t, mt)
	require.Equal(t, 3, db.nextMemFid)
}

func TestOpenMemtable(t *testing.T) {
	dir := utils.CreateTmpDir("memtable-test")
	defer utils.DestroyDir(dir)

	opts := config.DefaultOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	mt, err := db.newMemTable()
	require.NoError(t, err)
	require.NotNil(t, mt)
	require.Equal(t, 3, db.nextMemFid)

	db.nextMemFid = 0
	err = db.openMemTables()
	require.NoError(t, err)
	require.Equal(t, 3, db.nextMemFid)
}

func TestMemtablePut(t *testing.T) {
	dir := utils.CreateTmpDir("memtable-test")
	defer utils.DestroyDir(dir)

	opts := config.DefaultOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	mt, err := db.newMemTable()
	require.NoError(t, err)
	require.NotNil(t, mt)
	require.Equal(t, 3, db.nextMemFid)

	n := 100
	for i := n - 1; i >= 0; i-- {
		key := fmt.Sprintf("%05d", i*10+5)
		value := structs.ValueStruct{Value: newValue(i), Meta: byte(i)}
		err = mt.Put(utils.KeyWithTs([]byte(key), 0), value)
		require.NoError(t, err)
	}
}
