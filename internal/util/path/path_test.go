package path

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertToBlobURL(t *testing.T) {
	type args struct {
		urlstr string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"file path",
			args{
				urlstr: "file:///path/to/",
			},
			"file:///path/to/?create_dir=true&metadata=skip",
		},
		{
			"azblob path",
			args{
				urlstr: "azblob://bucket/path/to/",
			},
			"azblob://bucket?prefix=path/to/",
		},
		{
			"gs path",
			args{
				urlstr: "gs://bucket/path/to/",
			},
			"gs://bucket?prefix=path/to/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertToBlobURL(tt.args.urlstr)
			assert.NoError(t, err)
			if got != tt.want {
				t.Errorf("ConvertToBlobURL() = %v, want %v", got, tt.want)
			}
		})
	}
}
