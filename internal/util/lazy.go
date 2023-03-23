package util

import (
	"sync"
	"sync/atomic"

	"github.com/rotisserie/eris"
)

type Lazy[T any] struct {
	m     sync.Mutex
	done  uint32
	value T
	err   error
	eval  func() (T, error)
}

func LazyValue[T any](f func() (T, error)) *Lazy[T] {
	l := &Lazy[T]{eval: f}
	return l
}

// Get runs the specified function only once, but all callers gets the same
// result from that single execution.
func (o *Lazy[T]) Get() (T, error) {
	if atomic.LoadUint32(&o.done) == 1 {
		if o.err != nil {
			o.err = eris.Wrap(o.err, "")
		}
		return o.value, o.err
	}

	o.m.Lock()
	defer o.m.Unlock()
	if o.done == 0 {
		defer atomic.StoreUint32(&o.done, 1)
		o.value, o.err = o.eval()
	}

	if o.err != nil {
		o.err = eris.Wrap(o.err, "")
	}
	return o.value, o.err
}
