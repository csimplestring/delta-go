package store

import (
	"context"
	"strings"
	"sync"

	"github.com/rotisserie/eris"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util/path"
	"github.com/csimplestring/delta-go/iter"
)

func NewFileLogStore(logDirUrl string) (*LocalStore, error) {
	// logDirUrl is like: file:///a/b/c
	blobURL, err := path.ConvertToBlobURL(logDirUrl)
	if err != nil {
		return nil, err
	}

	bucket, err := blob.OpenBucket(context.Background(), blobURL)
	if err != nil {
		return nil, err
	}
	// remove file:// protocol
	logDir := strings.TrimPrefix(logDirUrl, "file://")
	s := &baseStore{
		logDir: logDirUrl,
		bucket: bucket,
	}
	return &LocalStore{
		logBasePath: logDir,
		s:           s,
	}, nil

}

type LocalStore struct {
	logBasePath string
	s           *baseStore
	mu          sync.Mutex
}

func (l *LocalStore) Root() string {
	//return l.LogPath
	return ""
}

func (l *LocalStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	return relativePath("file", l.logBasePath, path)
}

func (l *LocalStore) Read(path string) (iter.Iter[string], error) {
	path, err := l.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return nil, err
	}

	return l.s.Read(path)
}

func (l *LocalStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {
	path, err := l.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return nil, err
	}

	return l.s.ListFrom(path)
}

func (l *LocalStore) Write(path string, iter iter.Iter[string], overwrite bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	path, err := l.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return err
	}

	if !overwrite {

		exist, err := l.s.bucket.Exists(context.Background(), path)
		if err != nil {
			return eris.Wrap(err, "something wrong when checking for existence of "+path)
		}
		if exist {
			return errno.FileAlreadyExists(path)
		}
	}

	return l.s.Write(path, iter, overwrite)
}

func (l *LocalStore) IsPartialWriteVisible(path string) bool {
	// rename is used for atomicity write
	return false
}

func (l *LocalStore) Exists(path string) (bool, error) {
	_, err := l.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (l *LocalStore) Create(path string) error {
	return l.s.Create(path)
}
