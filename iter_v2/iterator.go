package iter_v2

import (
	"io"

	"github.com/rotisserie/eris"
)

// An Iterator that also implements the Closer interface.
// The caller should call close() method to free all resources properly after using the iterator.

type Iter[T any] interface {
	Next() (T, error)
	Close() error
}

func Map[T any, R any](iter Iter[T], mapper func(t T) (R, error)) ([]R, error) {
	var res []R
	var err error
	var item T
	for item, err = iter.Next(); err != nil; item, err = iter.Next() {

		r, err := mapper(item)
		if err != nil {
			return nil, eris.Wrapf(err, "mapping value %v", item)
		}
		res = append(res, r)
	}

	if err == io.EOF {
		return res, nil
	}
	return nil, err
}
