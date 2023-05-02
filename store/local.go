package store

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	linq "github.com/ahmetb/go-linq/v3"
	"github.com/rotisserie/eris"

	deltaErrors "github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/iter"
)

type LocalStore struct {
	LogPath string
	mu      sync.Mutex
}

func (l *LocalStore) Root() string { return l.LogPath }

func (l *LocalStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	p, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	// path is like "file:///a/b/c"
	if len(p.Scheme) > 0 {
		// remove "file://"
		p.Scheme = ""
		return p.String(), nil
	}
	// path is like "/a/b/c
	if filepath.IsAbs(path) {
		return path, nil
	}

	return filepath.Join(l.LogPath, path), nil
}

func (l *LocalStore) Read(path string) (iter.Iter[string], error) {
	path, _ = l.ResolvePathOnPhysicalStore(path)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, deltaErrors.FileNotFound(path)
		}
		return nil, eris.Wrap(err, "local store read "+path)
	}

	return iter.FromReadCloser(file), nil
}

func (l *LocalStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {
	path, _ = l.ResolvePathOnPhysicalStore(path)

	parent, startFile := filepath.Split(path)
	stats, err := os.ReadDir(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, deltaErrors.FileNotFound(path)
		}
		return nil, eris.Wrap(err, "local store list "+parent)
	}

	var res []*FileMeta
	linq.From(stats).Where(func(x interface{}) bool {
		n := x.(os.DirEntry)
		return !n.IsDir() && strings.Compare(n.Name(), startFile) >= 0
	}).Select(func(x interface{}) interface{} {
		n := x.(os.DirEntry)
		info, _ := n.Info()
		return &FileMeta{
			path:         filepath.Join(parent, n.Name()),
			timeModified: info.ModTime(),
			size:         uint64(info.Size()),
		}
	}).Sort(func(i, j interface{}) bool {
		return strings.Compare(i.(*FileMeta).path, j.(*FileMeta).path) < 0
	}).ToSlice(&res)

	return iter.FromSlice(res), nil
}

func (l *LocalStore) Write(path string, iter iter.Iter[string], overwrite bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	path, _ = l.ResolvePathOnPhysicalStore(path)

	if !overwrite {
		if _, err := os.Stat(path); err == nil {
			return deltaErrors.FileAlreadyExists(path)
		}
	}

	var err error
	w, err := newAtomicWriter(path)
	if err != nil {
		return err
	}

	var line string
	for line, err = iter.Next(); err == nil; line, err = iter.Next() {
		if _, werr := w.Write([]byte(line + "\n")); werr != nil {
			return werr
		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	return w.Close()
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
