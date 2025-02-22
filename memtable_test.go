package tiny_badger

import (
	"github.com/stretchr/testify/require"
	"testing"
	"tiny-badger/config"
	"tiny-badger/utils"
)

func TestNewMemtable(t *testing.T) {
	dir := utils.CreateTmpDir("memtable-test")
	defer utils.DestroyDir(dir)

	opts := config.DefaultOptions(dir)
	db, err := Open(opts)
	require.NoError(t, err)
	require.NotNil(t, db)

	mt, err := db.newMemTable()
	require.NoError(t, err)
	require.NotNil(t, mt)
	require.Equal(t, 1, db.nextMemFid)
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
	require.Equal(t, 1, db.nextMemFid)

	db.nextMemFid = 0
	err = db.openMemTables()
	require.NoError(t, err)
	require.Equal(t, 1, db.nextMemFid)
}
