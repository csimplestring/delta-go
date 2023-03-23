package util

import "github.com/mitchellh/hashstructure/v2"

func Hash(v interface{}) (uint64, error) {
	return hashstructure.Hash(v, hashstructure.FormatV2, nil)
}

func MustHash(v interface{}) uint64 {
	ret, err := hashstructure.Hash(v, hashstructure.FormatV2, nil)
	if err != nil {
		panic(err)
	}
	return ret
}
