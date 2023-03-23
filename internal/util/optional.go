package util

import "github.com/samber/mo"

func MapOptional[S any, D any](m mo.Option[S], mapper func(v S) D) mo.Option[D] {
	v, ok := m.Get()
	if !ok {
		return mo.None[D]()
	}
	return mo.Some(mapper(v))
}

func GetMapValue[K comparable, V any](m map[K]V, key K, defaultVal V) V {
	if v, ok := m[key]; ok {
		return v
	}
	return defaultVal
}

func GetMapValueOptional[K comparable, V any](m map[K]V, key K) mo.Option[V] {
	if val, ok := m[key]; ok {
		return mo.Some(val)
	}
	return mo.None[V]()
}

func OptionalToPtr[V any](o mo.Option[V]) *V {
	if o.IsAbsent() {
		return nil
	}

	v := o.MustGet()
	return &v
}

func OptionalFilter[V any](m mo.Option[V], filter func(V) bool) mo.Option[V] {
	if m.IsAbsent() {
		return mo.None[V]()
	}
	if filter(m.MustGet()) {
		return m
	}
	return mo.None[V]()
}

func OptionalFilterToPtr[V any](m mo.Option[V], filter func(V) bool) *V {
	if m.IsAbsent() {
		return nil
	}
	if filter(m.MustGet()) {
		return OptionalToPtr(m)
	}
	return nil
}
