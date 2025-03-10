package tiny_badger

import (
	"sync"
	"sync/atomic"
	"tiny-badger/structs"
)

type request struct {
	Entries []*structs.Entry
	Wg      sync.WaitGroup
	Err     error
	ref     atomic.Int32
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
	req.Wg = sync.WaitGroup{}
	req.Err = nil
	req.ref.Store(0)
}

func (req *request) IncrRef() {
	req.ref.Add(1)
}

func (req *request) DecrRef() {
	ref := req.ref.Add(-1)
	if ref > 0 {
		return
	}
	req.Entries = nil
	requestPool.Put(req)
}

func (req *request) Wait() error {
	req.Wg.Wait()
	err := req.Err
	req.DecrRef()
	return err
}
