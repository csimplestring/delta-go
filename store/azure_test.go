package store

import (
	"os"
	"testing"

	"github.com/csimplestring/delta-go/iter"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/azureblob"
)

func TestAzureBlobLogStore(t *testing.T) {
	os.Setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
	os.Setenv("AZURE_STORAGE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

	// running the Azurite local emulator
	os.Setenv("AZURE_STORAGE_DOMAIN", "localhost:10000")
	os.Setenv("AZURE_STORAGE_PROTOCOL", "http")
	os.Setenv("AZURE_STORAGE_IS_CDN", "false")
	os.Setenv("AZURE_STORAGE_IS_LOCAL_EMULATOR", "true")

	path := "azblob://golden/checkpoint/_delta_log/"

	s, err := NewAzureBlobLogStore(path)
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
