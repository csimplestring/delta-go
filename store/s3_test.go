package store

import (
	"os"
	"testing"

	"github.com/csimplestring/delta-go/iter"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/blob/s3blob"
)

func TestS3LogStore(t *testing.T) {
	os.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
	os.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	os.Setenv("AWS_DISABLE_SSL", "true")
	os.Setenv("AWS_S3_FORCE_PATH_STYLE", "true")
	os.Setenv("AWS_ACCESS_KEY_ID", "foo")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	path := "s3://golden/checkpoint/_delta_log/"

	s, err := NewS3LogStore(path)
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
