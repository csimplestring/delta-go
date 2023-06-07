package store

import (
	"context"
	"strings"
	"sync"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util/path"
	"github.com/csimplestring/delta-go/iter"
	"github.com/rotisserie/eris"

	goblob "gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob"
)

func NewS3LogStore(logDir string) (*S3LogStore, error) {
	// logDir is like: s3:///a/b/c/_delta_log/, must end with "/"
	blobURL, err := path.ConvertToBlobURL(logDir)
	if err != nil {
		return nil, err
	}

	bucket, err := goblob.OpenBucket(context.Background(), blobURL)
	if err != nil {
		return nil, err
	}

	logDir = strings.TrimPrefix(logDir, "s3://")
	s := &baseStore{
		logDir: logDir,
		bucket: bucket,
		beforeWriteFn: func(asFunc func(interface{}) bool) error {
			return nil
		},
		writeErrorFn: func(err error, path string) error {
			return err
		},
	}

	return &S3LogStore{
		logDir: logDir,
		s:      s,
	}, nil
}

type S3LogStore struct {
	logDir string
	s      *baseStore
	mu     sync.Mutex
}

func (a *S3LogStore) Root() string {
	return ""
}

// Read the given file and return an `Iterator` of lines, with line breaks removed from
// each line. Callers of this function are responsible to close the iterator if they are
// done with it.
func (a *S3LogStore) Read(path string) (iter.Iter[string], error) {
	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return nil, err
	}

	return a.s.Read(path)
}

// List the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
func (a *S3LogStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {
	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return nil, err
	}

	return a.s.ListFrom(path)
}

// Write the given `actions` to the given `path` with or without overwrite as indicated.
// Implementation must throw FileAlreadyExistsException exception if the file already
// exists and overwrite = false. Furthermore, if isPartialWriteVisible returns false,
// implementation must ensure that the entire file is made visible atomically, that is,
// it should not generate partial files.
func (a *S3LogStore) Write(path string, actions iter.Iter[string], overwrite bool) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return err
	}

	if !overwrite {
		ok, err := a.s.Exists(path)
		if err != nil {
			return eris.Wrap(err, "s3 failed to check existing file "+path)
		}
		if ok {
			return errno.FileAlreadyExists(path)
		}
	}

	return a.s.Write(path, actions, overwrite)
}

// Resolve the fully qualified path for the given `path`.
func (a *S3LogStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	return relativePath("s3", a.logDir, path)
}

// Whether a partial write is visible for the underlying file system of `path`.
func (a *S3LogStore) IsPartialWriteVisible(path string) bool {
	return false
}

func (a *S3LogStore) Exists(path string) (bool, error) {
	return a.s.Exists(path)
}

func (a *S3LogStore) Create(path string) error {
	return a.s.Create(path)
}
