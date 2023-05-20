package store

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rotisserie/eris"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/iter"
)

func buildLocalUrl(logDirUrl string) (string, error) {
	p, err := url.Parse(logDirUrl)
	if err != nil {
		return "", eris.Wrap(err, "could not parse local log dir url")
	}
	q := p.Query()
	q.Set("create_dir", "true")
	q.Set("metadata", "skip")
	p.RawQuery = q.Encode()

	return p.String(), nil
}

func NewFileLogStore(logDirUrl string) (*LocalStore, error) {
	// logDir is like: file://a/b/c
	urlstr, err := buildLocalUrl(logDirUrl)
	if err != nil {
		return nil, err
	}

	bucket, err := blob.OpenBucket(context.Background(), urlstr)
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
		logPath: logDir,
		s:       s,
	}, nil

}

type LocalStore struct {
	logPath string
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

	cleanLogPath := strings.TrimSuffix(l.logPath, "/")
	cleanDir := strings.TrimSuffix(dir, "/")
	if cleanLogPath != cleanDir {
		return "", eris.Errorf("the configured log dir is %s but the provided log dir is %s", l.logPath, dir)
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

	if !overwrite {
		if _, err := os.Stat(filepath.Join(l.logPath, path)); err == nil {
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
	return l.s.Exists(path)
}

func (l *LocalStore) Create(path string) error {
	return l.s.Create(path)
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
