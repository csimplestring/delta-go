package store

import (
	"net/url"
	"os"
	"strings"

	"github.com/csimplestring/delta-go/errno"
	"github.com/rotisserie/eris"
)

var _ FS = &LocalFS{}

type FS interface {
	Exists(path string) (bool, error)
	Mkdirs(path string) error
	Create(path string, overwrite bool) error
}

type LocalFS struct {
}

func (l *LocalFS) Mkdirs(path string) error {
	path = strings.TrimPrefix(path, "file://")
	return os.Mkdir(path, os.ModePerm)
}

func (l *LocalFS) Exists(path string) (bool, error) {
	path = strings.TrimPrefix(path, "file://")
	_, err := os.Stat(path)
	return err == nil, nil
}

func (l *LocalFS) Create(path string, overwrite bool) error {
	path = strings.TrimPrefix(path, "file://")

	exist, err := l.Exists(path)
	if err != nil {
		return err
	}
	if !exist {
		f, err := os.Create(path)
		if err != nil {
			return eris.Wrap(err, "local filesystem creation "+path)
		}
		defer f.Close()
		return nil
	}

	// path exists
	flag := os.O_APPEND
	if overwrite {
		flag = os.O_TRUNC
	}
	f, err := os.OpenFile(path, flag, os.ModePerm)
	if err != nil {
		return eris.Wrap(err, "local filesystem open "+path)
	}
	defer f.Close()
	return nil
}

func GetFileSystem(path string) (FS, error) {
	p, err := url.Parse(path)
	if err != nil {
		return nil, eris.Wrapf(err, "error in parsing %s for file system", path)
	}

	if p.Scheme == "file" {
		return &LocalFS{}, nil
	}

	return nil, errno.UnsupportedFileSystem("msg string")
}
