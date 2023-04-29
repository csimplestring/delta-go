package iter_v2

type MapIter[T any, R any] struct {
	It     Iter[T]
	Mapper func(t T) (R, error)
}

func (d *MapIter[T, R]) Next() (R, error) {
	var res R
	t, err := d.It.Next()
	if err != nil {
		return res, err
	}
	return d.Mapper(t)
}

func (a *MapIter[T, R]) Close() error {
	return a.It.Close()
}
