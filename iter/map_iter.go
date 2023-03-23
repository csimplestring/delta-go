package iter

type MapIter[T any, R any] struct {
	It     Iter[T]
	Mapper func(t T) (R, error)
}

func (d *MapIter[T, R]) Next() bool {
	return d.It.Next()
}

func (d *MapIter[T, R]) Value() (R, error) {
	var res R
	t, err := d.It.Value()
	if err != nil {
		return res, err
	}
	return d.Mapper(t)
}

func (a *MapIter[T, R]) Close() error {
	return a.It.Close()
}
