package tiny_badger

import (
	"tiny-badger/structs"
	"tiny-badger/utils"
)

type levelsController struct {
	db *DB

	levels []*levelHandler
}

func (lc *levelsController) Get(key []byte, maxVs structs.ValueStruct, startLevel int) (structs.ValueStruct, error) {
	if lc.db.IsClosed() {
		return structs.ValueStruct{}, utils.ErrDBClosed
	}

	version := utils.ParseTs(key)
	for _, l := range lc.levels {
		if l.level < startLevel {
			continue
		}
		vs, err := l.get(key)
		if err != nil {
			return structs.ValueStruct{}, utils.Wrapf(err, "get key: %q", key)
		}
		if vs.Value == nil && vs.Meta == 0 {
			continue
		}
		if vs.Version == version {
			return vs, nil
		}
		if maxVs.Version < vs.Version {
			maxVs = vs
		}
	}
	return maxVs, nil
}
