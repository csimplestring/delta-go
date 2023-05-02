package iter

// import (
// 	"github.com/rotisserie/eris"
// )

// var _ Iter[string] = &SliceIter[string]{}

// type SliceIter[T any] struct {
// 	actions []T
// 	i       int
// }

// func (a *SliceIter[T]) Next() bool {
// 	return a.i < len(a.actions)
// }

// func (a *SliceIter[T]) Value() (T, error) {
// 	var item T
// 	if a.i < len(a.actions) {
// 		item = a.actions[a.i]
// 		a.i++
// 		return item, nil
// 	}
// 	return item, nil
// }

// func (a *SliceIter[T]) Close() error {
// 	return nil
// }

// func FromSlice[T any](s []T) Iter[T] {
// 	return &SliceIter[T]{
// 		actions: s,
// 		i:       0,
// 	}
// }

// func ToSlice[T any](iter Iter[T]) ([]T, error) {
// 	if iter == nil {
// 		return nil, eris.New("nil iterator")
// 	}

// 	var s []T
// 	for iter.Next() {
// 		item, err := iter.Value()
// 		if err != nil {
// 			return nil, err
// 		}
// 		s = append(s, item)
// 	}
// 	iter.Close()
// 	return s, nil
// }
