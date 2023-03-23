package path

import (
	"testing"
)

func TestQualified(t *testing.T) {
	type args struct {
		base string
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"test1",
			args{base: "file:///a", path: "b.json"},
			"file:///a/b.json",
			false,
		},
		{
			"test1",
			args{base: "file:///a", path: "b/c.json"},
			"file:///a/b/c.json",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Qualified(tt.args.base, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("Qualified() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Qualified() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRelative(t *testing.T) {
	type args struct {
		base string
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"test1",
			args{base: "file:///a", path: "a/b/c"},
			"a/b/c",
			false,
		},
		{
			"test2",
			args{base: "file:///a", path: "file:///a/b/c"},
			"b/c",
			false,
		},
		{
			"test3",
			args{base: "file:///a", path: "file:///b/c"},
			"file:///b/c",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Relative(tt.args.base, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("Relative() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Relative() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCanonicalize(t *testing.T) {
	type args struct {
		path   string
		schema string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "local file path with schema",
			args: args{
				path:   "file:///a/b/c",
				schema: "file",
			},
			want:    "file:///a/b/c",
			wantErr: false,
		},
		{
			name: "local file path without schema",
			args: args{
				path:   "/a/b/c",
				schema: "file",
			},
			want:    "file:///a/b/c",
			wantErr: false,
		},
		{
			name: "local file path with non-standard schema",
			args: args{
				path:   "file:/a/b/c",
				schema: "file",
			},
			want:    "file:///a/b/c",
			wantErr: false,
		},
		{
			name: "local file relative path ",
			args: args{
				path:   "./a/b/c",
				schema: "file",
			},
			want:    "./a/b/c",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Canonicalize(tt.args.path, tt.args.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("Canonicalize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Canonicalize() = %v, want %v", got, tt.want)
			}
		})
	}
}
