package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rotisserie/eris"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"

	"github.com/csimplestring/delta-go/iter"
)

func newLocalStore(logDir string) (*baseStore, error) {
	url := fmt.Sprintf("file://%s?create_dir=true", logDir)
	bucket, err := blob.OpenBucket(context.Background(), url)
	if err != nil {
		return nil, err
	}
	return &baseStore{
		logDir: logDir,
		bucket: bucket,
	}, nil
}

type LocalStore struct {
	LogPath string
	s       *baseStore
	mu      sync.Mutex
}

func (l *LocalStore) Root() string {
	//return l.LogPath
	return ""
}

func (l *LocalStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	path = strings.TrimPrefix(path, "file://")
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	// relative path
	if dir == "." {
		return base, nil
	}

	if strings.TrimSuffix(l.LogPath, "/") != strings.TrimSuffix(dir, "/") {
		return "", eris.Errorf("the configured log dir is %s but the provided log dir is %s", l.LogPath, dir)
	}
	return base, nil
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

	return l.s.Write(path, iter, overwrite)
}

func (l *LocalStore) IsPartialWriteVisible(path string) bool {
	// rename is used for atomicity write
	return false
}

type atomicWriter struct {
	name string
	tf   *os.File
}

func newAtomicWriter(name string) (io.WriteCloser, error) {
	// we already check the named file does not exist
	if _, err := os.Create(name); err != nil {
		return nil, eris.Wrap(err, name)
	}

	tf, err := os.CreateTemp(os.TempDir(), "deltago")
	if err != nil {
		return nil, eris.Wrap(err, "")
	}
	return &atomicWriter{name, tf}, nil
}

// Write() writes data into the file. Bytes will be written to the temporary file.
func (aw *atomicWriter) Write(b []byte) (int, error) {
	return aw.tf.Write(b)
}

// Close() flushes the temporary file, closes it and renames to the original
// filename.
func (aw *atomicWriter) Close() error {
	var ret error
	if err := aw.tf.Sync(); err != nil {
		ret = eris.Wrap(err, "sync error")
	}
	if err := aw.tf.Close(); err != nil {
		ret = eris.Wrap(err, "close error")
	}
	if err := os.Rename(aw.tf.Name(), aw.name); err != nil {
		ret = eris.Wrap(err, "rename error")
	}
	if err := os.RemoveAll(aw.tf.Name()); err != nil {
		ret = eris.Wrap(err, "remove error")
	}
	return ret
}
