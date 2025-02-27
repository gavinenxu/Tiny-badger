package tiny_badger

import (
	"sync"
	"tiny-badger/structs"
)

type request struct {
	Entries []*structs.Entry
	Wg      sync.WaitGroup
	Err     error
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Wg = sync.WaitGroup{}
	req.Err = nil
}
