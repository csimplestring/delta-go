package util

func PtrOf[T any](t T) *T {
	return &t
}
