package store

import (
	"context"
	"strings"

	"github.com/csimplestring/delta-go/iter"
	"gocloud.dev/blob"
)

type baseStore struct {
	logDir        string
	bucket        *blob.Bucket
	beforeWriteFn func(asFunc func(interface{}) bool) error
	writeErrorFn  func(err error, path string) error
}

func (b *baseStore) Read(path string) (iter.Iter[string], error) {
	// path is relative to the root log path, do NOT start with '/'
	path = strings.TrimPrefix(path, b.logDir)
	r, err := b.bucket.NewReader(context.Background(), path, nil)
	if err != nil {
		return nil, err
	}

	return iter.FromReadCloser(r), nil
}

func (b *baseStore) Write(path string, actions iter.Iter[string], overwrite bool) error {
	path = strings.TrimPrefix(path, b.logDir)
	var writeOpt *blob.WriterOptions

	if !overwrite {
		writeOpt = &blob.WriterOptions{
			BeforeWrite: b.beforeWriteFn,
		}
	}

	w, err := b.bucket.NewWriter(context.Background(), path, writeOpt)
	if err != nil {
		return err
	}

	if _, err = w.ReadFrom(iter.AsReadCloser(actions, true)); err != nil {
		return err
	}

	if err := w.Close(); err != nil {
		return b.writeErrorFn(err, path)
	}

	return actions.Close()
}

func (b *baseStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {

	return newListingIter(b.logDir, path, b.bucket)
}

type listingIter struct {
	startPath string
	logDir    string
	bucket    *blob.Bucket
	pageToken []byte
	buffer    []*blob.ListObject
}

func newListingIter(logDir string, startPath string, bucket *blob.Bucket) (*listingIter, error) {

	return &listingIter{
		startPath: startPath,
		logDir:    logDir,
		bucket:    bucket,
		pageToken: blob.FirstPageToken,
	}, nil
}

var _ iter.Iter[*FileMeta] = &listingIter{}

func (l *listingIter) Next() (*FileMeta, error) {

	if len(l.buffer) == 0 {
		ret, nextPageToken, err := l.bucket.ListPage(context.Background(), l.pageToken, 500, nil)
		if err != nil {
			return nil, err
		}

		l.pageToken = nextPageToken
		l.buffer = ret
		return l.Next()
	}

	v, buffer := l.buffer[0], l.buffer[1:]
	l.buffer = buffer

	if v.Key < l.startPath {
		return l.Next()
	}

	return &FileMeta{
		path:         v.Key,
		size:         uint64(v.Size),
		timeModified: v.ModTime,
	}, nil

}

func (l *listingIter) Close() error {
	return nil
}

func (b *baseStore) Exists(path string) (bool, error) {
	return b.bucket.Exists(context.Background(), path)
}

func (b *baseStore) Create(path string) error {
	return b.bucket.WriteAll(context.Background(), path, []byte{}, nil)
}
