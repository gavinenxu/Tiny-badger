package tiny_badger

import "tiny-badger/structs"

type levelHandler struct {
	level int
}

func (s *levelHandler) get(key []byte) (structs.ValueStruct, error) {
	return structs.ValueStruct{}, nil
}
