package store

import (
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/iter"
	"github.com/rotisserie/eris"
)

// General interface for all critical file system operations required to read and write
// the Delta logs. The correctness is predicated on the atomicity and durability guarantees
// of the implementation of this interface. Specifically,
// Atomic visibility of files: If isPartialWriteVisible is false, any file written through
// this store must be made visible atomically. In other words, this should not generate partial
// files.
// Mutual exclusion: Only one writer must be able to create (or rename) a file at the final
// destination.
// Consistent listing: Once a file has been written in a directory, all future listings for
// that directory must return that file.
// All subclasses of this interface is required to have a constructor that takes Configuration
// as a single parameter. This constructor is used to dynamically create the Store.
// Store and its implementations are not meant for direct access but for configuration based
// on storage system. See [[https://docs.delta.io/latest/delta-storage.html]] for details.
type Store interface {
	Root() string

	// Read the given file and return an `Iterator` of lines, with line breaks removed from
	// each line. Callers of this function are responsible to close the iterator if they are
	// done with it.
	Read(path string) (iter.Iter[string], error)

	// List the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
	ListFrom(path string) (iter.Iter[*FileMeta], error)

	// Write the given `actions` to the given `path` with or without overwrite as indicated.
	// Implementation must throw FileAlreadyExistsException exception if the file already
	// exists and overwrite = false. Furthermore, if isPartialWriteVisible returns false,
	// implementation must ensure that the entire file is made visible atomically, that is,
	// it should not generate partial files.
	Write(path string, actions iter.Iter[string], overwrite bool) error

	// Resolve the fully qualified path for the given `path`.
	ResolvePathOnPhysicalStore(path string) (string, error)

	// Whether a partial write is visible for the underlying file system of `path`.
	IsPartialWriteVisible(path string) bool

	Exists(path string) (bool, error)

	Create(path string) error
}

type FileMeta struct {
	path         string
	timeModified time.Time
	size         uint64
}

func (f *FileMeta) Path() string {
	return f.path
}

func (f *FileMeta) TimeModified() time.Time {
	return f.timeModified
}

func (f *FileMeta) Size() uint64 {
	return f.size
}

func New(path string) (Store, error) {
	p, err := url.Parse(path)
	if err != nil {
		return nil, eris.Wrapf(err, "error in parsing %s for Store", path)
	}

	if p.Scheme == "file" {
		return NewFileLogStore(path)
	} else if p.Scheme == "azblob" {
		return NewAzureBlobLogStore(path)
	} else if p.Scheme == "gs" {
		return NewGCSLogStore(path)
	} else if p.Scheme == "s3" {
		return NewS3LogStore(path)
	}

	return nil, errno.UnsupportedFileSystem("unsupported schema " + path + " to create log store")
}

func relativePath(scheme string, basePath string, path string) (string, error) {
	rel := func(base string, path string) (string, error) {
		relativePath, err := filepath.Rel(basePath, path)
		if err != nil {
			return "", eris.Wrap(err, "fail to resolve the relative path for "+path)
		}
		// path is not in the basePath
		if strings.HasPrefix(relativePath, "../") {
			return "", eris.Errorf("the path %s is not in the base path %s", path, basePath)
		}
		return relativePath, nil
	}

	// absolute path: "file:///path/to/file"
	if strings.HasPrefix(path, scheme+"://") {
		path = strings.TrimPrefix(path, scheme+"://")
		return rel(basePath, path)
	}

	// absolute path without scheme "/path/to/a"
	if !strings.HasPrefix(path, scheme+"://") && filepath.IsAbs(path) {
		return rel(basePath, path)
	}

	// relative path
	if !strings.HasPrefix(path, scheme+"://") && !filepath.IsAbs(path) {
		return path, nil
	}

	return "", eris.Errorf("fail to resolve the relative path for %s with basePath %s", path, basePath)
}
