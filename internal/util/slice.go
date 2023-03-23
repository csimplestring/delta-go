package util

import mapset "github.com/deckarep/golang-set/v2"

func Exists[T any](elements []T, predicate func(e T) bool) bool {
	for _, e := range elements {
		if predicate(e) {
			return true
		}
	}
	return false
}

func Keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func KeySet[K comparable, V any](m map[K]V) mapset.Set[K] {
	return mapset.NewSet(Keys(m)...)
}
