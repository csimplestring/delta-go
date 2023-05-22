package util

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
)

func Test_LocalCopyDir(t *testing.T) {
	path, err := filepath.Abs("../../tests/golden/")
	assert.NoError(t, err)
	urlstr := fmt.Sprintf("file://%s?metadata=skip", path)

	dir, err := CopyBlobDir(urlstr, "checkpoint")
	assert.NoError(t, err)

	err = DelBlobFiles(urlstr, dir)
	assert.NoError(t, err)
}
