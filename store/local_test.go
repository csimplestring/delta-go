package store

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/csimplestring/delta-go/iter"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
)

func TestLocalStore(t *testing.T) {
	p, err := filepath.Abs("../tests/golden/checkpoint/_delta_log/")
	assert.NoError(t, err)

	s, err := NewFileLogStore(fmt.Sprintf("file://%s", p))
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

func Test_relativePath(t *testing.T) {

	tests := []struct {
		scheme   string
		base     string
		path     string
		expected string
	}{
		{
			"file", "/a/b/c/_delta_log/", "file:///a/b/c/_delta_log/0.json", "0.json",
		},
		{
			"file", "/a/b/c/_delta_log/", "file:///a/b/c/_delta_log/d/0.json", "d/0.json",
		},
		{
			"file", "/a/b/c/_delta_log/", "/a/b/c/_delta_log/0.json", "0.json",
		},
		{
			"file", "/a/b/c/_delta_log/", "/a/b/c/_delta_log/d/0.json", "d/0.json",
		},
		{
			"file", "/a/b/c/_delta_log/", "0.json", "0.json",
		},
		{
			"file", "/a/b/c/_delta_log/", "d/0.json", "d/0.json",
		},
	}
	for _, tt := range tests {
		r, err := relativePath(tt.scheme, tt.base, tt.path)
		assert.NoError(t, err)
		assert.Equal(t, tt.expected, r)
	}

	r, err := relativePath("file", "/a/b/c/_delta_log/", "file:///a/b/c/0.json")
	assert.ErrorContains(t, err, "is not in the base path")
	assert.Equal(t, "", r)

	r, err = relativePath("file", "/a/b/c/_delta_log/", "file:///a/0.json")
	assert.ErrorContains(t, err, "is not in the base path")
	assert.Equal(t, "", r)

	r, err = relativePath("file", "/a/b/c/_delta_log/", "/a/b/c/0.json")
	assert.ErrorContains(t, err, "is not in the base path")
	assert.Equal(t, "", r)
}
