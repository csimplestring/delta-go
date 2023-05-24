package util

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
)

func Test_LocalBlobDir(t *testing.T) {
	
	path, err := filepath.Abs("../../tests/golden/")
	assert.NoError(t, err)
	urlstr := fmt.Sprintf("file://%s?metadata=skip", path)

	bd, err := NewBlobDir(urlstr)
	assert.NoError(t, err)

	// create temp folder
	tempDir, placeHolder, err := bd.CreateTemp()
	assert.NoError(t, err)
	assert.NotEmpty(t, placeHolder)
	assert.NotEmpty(t, tempDir)

	// clean up temp folder
	err = bd.Delete(tempDir, []string{placeHolder}, true)
	assert.NoError(t, err)

	// copy from a folder
	newDir, files, err := bd.Copy("checkpoint")
	assert.NoError(t, err)
	assert.Equal(t, 18, len(files))

	err = bd.Delete(newDir, files, true)
	assert.NoError(t, err)
}
