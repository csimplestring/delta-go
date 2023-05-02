package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/csimplestring/delta-go/iter_v2"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
)

func Test_newLocalStore(t *testing.T) {
	p, err := filepath.Abs("../tests/golden/checkpoint/_delta_log/")
	assert.NoError(t, err)

	s, err := newLocalStore(p)
	assert.NoError(t, err)

	i, err := s.Read("00000000000000000014.json")
	assert.NoError(t, err)

	for line, err := i.Next(); err == nil; line, err = i.Next() {
		fmt.Println(line)
	}

	iter, err := s.ListFrom("00000000000000000007.json")
	assert.NoError(t, err)

	files, err := iter_v2.Map(iter, func(f *FileMeta) (string, error) {
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
	}, files)
}

func Test_newAzureBlobStore(t *testing.T) {
	os.Setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
	os.Setenv("AZURE_STORAGE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

	container := "golden"
	logDir := "checkpoint/_delta_log/"
	isEmulator := true
	s, err := newAzureBlobStore(container, logDir, isEmulator)
	assert.NoError(t, err)

	i, err := s.Read("00000000000000000014.json")
	assert.NoError(t, err)
	for v, err := i.Next(); err == nil; v, err = i.Next() {
		fmt.Println(v)
	}
}

func Test_prefix(t *testing.T) {
	prefixes := yieldVersionPrefix()
	startPos := lookupPrefixPos(fmt.Sprintf("%020d.json", 623), prefixes)

	for i := startPos; i < len(prefixes); i++ {
		//fmt.Println(prefixes[i])
	}
}
