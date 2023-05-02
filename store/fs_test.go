package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
	for i.Next() {
		fmt.Println(i.Value())
	}

	iter, err := s.ListFrom("00000000000000000011.json")
	assert.NoError(t, err)
	for iter.Next() {
		fmt.Println(iter.Value())
	}
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
	for i.Next() {
		fmt.Println(i.Value())
	}
}

func Test_prefix(t *testing.T) {
	prefixes := yieldVersionPrefix()
	startPos := lookupPrefixPos(fmt.Sprintf("%020d.json", 623), prefixes)

	for i := startPos; i < len(prefixes); i++ {
		//fmt.Println(prefixes[i])
	}
}
