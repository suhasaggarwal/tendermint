package fnConsensus

import (
	"errors"
	"sync"
)

var ErrFnIDIsTaken = errors.New("FnID is already used by another Fn Object")

type Fn interface {
	GetNonce() (int64, error)
	SubmitMultiSignedMessage(message []byte, signatures [][]byte)
	GetMessage() ([]byte, error)
}

// Transient registry, need to rebuild upon restart
type FnRegistry struct {
	mtx   sync.RWMutex
	fnMap map[string]Fn
}

func NewFnRegistry() *FnRegistry {
	return &FnRegistry{
		fnMap: make(map[string]Fn),
	}
}

func (f *FnRegistry) Get(fnID string) Fn {
	f.mtx.RLock()
	defer f.mtx.RUnlock()
	return f.fnMap[fnID]
}

func (f *FnRegistry) Set(fnID string, fnObj Fn) error {
	f.mtx.Lock()
	defer f.mtx.Unlock()

	_, exists := f.fnMap[fnID]
	if exists {
		return ErrFnIDIsTaken
	}

	f.fnMap[fnID] = fnObj
	return nil
}
