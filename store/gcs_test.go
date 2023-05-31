package store

import (
	"os"
	"testing"

	"github.com/csimplestring/delta-go/iter"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/azureblob"
)

func TestGCSBlobLogStore(t *testing.T) {
	// test with local emulator
	os.Setenv("STORAGE_EMULATOR_HOST", "localhost:4443")

	path := "gs://golden/checkpoint/_delta_log/"

	s, err := NewGCSLogStore(path)
	assert.NoError(t, err)

	data, err := s.Read("00000000000000000000.json")
	assert.NoError(t, err)

	sl, err := iter.ToSlice(data)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(sl))

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
