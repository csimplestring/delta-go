package iter

import (
	"io"

	"github.com/rotisserie/eris"
)

var _ Iter[string] = &SliceIter[string]{}

type SliceIter[T any] struct {
	actions []T
	i       int
}

func (a *SliceIter[T]) Next() (T, error) {
	var item T
	if a.i < len(a.actions) {
		item = a.actions[a.i]
		a.i++
		return item, nil
	}
	return item, io.EOF
}

func (a *SliceIter[T]) Close() error {
	return nil
}

func FromSlice[T any](s []T) Iter[T] {
	return &SliceIter[T]{
		actions: s,
		i:       0,
	}
}

func ToSlice[T any](iter Iter[T]) ([]T, error) {
	if iter == nil {
		return nil, eris.New("nil iterator")
	}
	defer iter.Close()

	var s []T
	var item T
	var err error
	for item, err = iter.Next(); err == nil; item, err = iter.Next() {
		s = append(s, item)
	}
	if err != nil && err != io.EOF {
		return nil, err
	}

	return s, nil
}
