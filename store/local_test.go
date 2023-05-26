package store

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/csimplestring/delta-go/iter"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/fileblob"
)

func TestLocalStore_ListFrom(t *testing.T) {
	p, err := filepath.Abs("../tests/golden/checkpoint/_delta_log/")
	assert.NoError(t, err)

	s, err := NewFileLogStore(fmt.Sprintf("file://%s", p))
	assert.NoError(t, err)

	it, err := s.ListFrom("00000000000000000007.json")
	assert.NoError(t, err)

	files, err := iter.Map(it, func(f *FileMeta) (string, error) {
		return f.path, nil
	})

	assert.NoError(t, err)

	assert.Equal(t, []string{
		"00000000000000000007.json",
		"00000000000000000008.json",
		"00000000000000000009.json",
		"00000000000000000010.checkpoint.parquet",
		"00000000000000000010.json",
		"00000000000000000011.json",
		"00000000000000000012.json",
		"00000000000000000013.json",
		"00000000000000000014.json",
		"_last_checkpoint",
	}, files)
}
