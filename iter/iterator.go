package iter

import "github.com/rotisserie/eris"

// An Iterator that also implements the Closer interface.
// The caller should call close() method to free all resources properly after using the iterator.

type Iter[T any] interface {
	Next() bool
	Value() (T, error)
	Close() error
}

func Map[T any, R any](iter Iter[T], mapper func(t T) (R, error)) ([]R, error) {
	var res []R
	for iter.Next() {
		v, err := iter.Value()
		if err != nil {
			return nil, eris.Wrap(err, "retrieve value from iterator")
		}
		r, err := mapper(v)
		if err != nil {
			return nil, eris.Wrapf(err, "mapping value %v", v)
		}
		res = append(res, r)
	}
	return res, nil
}
