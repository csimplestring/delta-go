package store

import (
	"context"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util/path"
	"github.com/csimplestring/delta-go/iter"

	goblob "gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcerrors"
)

func NewGCSLogStore(logDir string) (*GCSLogStore, error) {
	// logDir is like: gs:///a/b/c/_delta_log/, must end with "/"
	blobURL, err := path.ConvertToBlobURL(logDir)
	if err != nil {
		return nil, err
	}

	bucket, err := goblob.OpenBucket(context.Background(), blobURL)
	if err != nil {
		return nil, err
	}

	logDir = strings.TrimPrefix(logDir, "gs://")
	s := &baseStore{
		logDir: logDir,
		bucket: bucket,
		beforeWriteFn: func(asFunc func(interface{}) bool) error {
			var handle **storage.ObjectHandle
			if asFunc(&handle) {
				(*handle) = (*handle).If(storage.Conditions{DoesNotExist: true})
			}
			return nil
		},
		writeErrorFn: func(err error, path string) error {
			if err == nil {
				return nil
			}
			if gcerrors.Code(err) == gcerrors.FailedPrecondition {
				return errno.FileAlreadyExists(path)
			}
			return err
		},
	}

	return &GCSLogStore{
		logDir: logDir,
		s:      s,
	}, nil
}

type GCSLogStore struct {
	logDir string
	s      *baseStore
}

func (a *GCSLogStore) Root() string {
	return ""
}

// Read the given file and return an `Iterator` of lines, with line breaks removed from
// each line. Callers of this function are responsible to close the iterator if they are
// done with it.
func (a *GCSLogStore) Read(path string) (iter.Iter[string], error) {
	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return nil, err
	}

	return a.s.Read(path)
}

// List the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
func (a *GCSLogStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {
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
func (a *GCSLogStore) Write(path string, actions iter.Iter[string], overwrite bool) error {

	path, err := a.ResolvePathOnPhysicalStore(path)
	if err != nil {
		return err
	}

	return a.s.Write(path, actions, overwrite)
}

// Resolve the fully qualified path for the given `path`.
func (a *GCSLogStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	return relativePath("gs", a.logDir, path)
}

// Whether a partial write is visible for the underlying file system of `path`.
func (a *GCSLogStore) IsPartialWriteVisible(path string) bool {
	return false
}

func (a *GCSLogStore) Exists(path string) (bool, error) {
	return a.s.Exists(path)
}

func (a *GCSLogStore) Create(path string) error {
	return a.s.Create(path)
}
