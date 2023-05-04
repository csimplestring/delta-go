package store

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/iter"
	"github.com/rotisserie/eris"
	"gocloud.dev/blob"
	"golang.org/x/exp/slices"
)

var prefilledListingPrefixes []string

func init() {
	prefilledListingPrefixes = yieldVersionPrefix()
}

func yieldVersionPrefix() []string {
	var prefixes []string
	seeds := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}

	s := fmt.Sprintf("%020d", 0)
	prefixes = append(prefixes, s)

	for k := 19; k >= 0; k-- {
		for i := 0; i < len(seeds); i++ {
			v := replaceAtIndex(s, seeds[i], k)
			prefixes = append(prefixes, strings.TrimRight(v, "0"))
		}
	}

	return prefixes
}

func replaceAtIndex(str string, replacement string, index int) string {
	return str[:index] + replacement + str[index+1:]
}

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
	// get prefix
	prefixYielder *listingPrefixYielder
	prefix        string
	// list objects
	bucket          *blob.Bucket
	blobListingIter *blob.ListIterator
}

func newListingIter(logDir string, startPath string, bucket *blob.Bucket) (*listingIter, error) {
	version := strings.Split(startPath, ".")[0]

	startPos := lookupPrefixPos(version, prefilledListingPrefixes)
	if startPos == -1 {
		return nil, eris.Wrap(errno.ErrFileNotFound, "cannot listing files starting with "+startPath)
	}
	yielder := &listingPrefixYielder{
		startPos: startPos,
		prefixes: prefilledListingPrefixes,
	}

	prefix, err := yielder.Next()
	if err != nil {
		return nil, eris.Wrap(errno.ErrIllegalArgument, "cannot generate prefix for "+startPath)
	}

	return &listingIter{
		startPath:       startPath,
		logDir:          logDir,
		prefixYielder:   yielder,
		prefix:          prefix,
		bucket:          bucket,
		blobListingIter: bucket.List(&blob.ListOptions{Prefix: prefix}),
	}, nil
}

var _ iter.Iter[*FileMeta] = &listingIter{}

func (l *listingIter) Next() (*FileMeta, error) {
	v, err := l.blobListingIter.Next(context.Background())
	if err == io.EOF {
		// need to reload prefix
		prefix, err := l.prefixYielder.Next()
		if err == io.EOF {
			return nil, err
		}
		// reload objects
		l.blobListingIter = l.bucket.List(&blob.ListOptions{Prefix: prefix})
		return l.Next()
	}
	if err != nil {
		return nil, err
	}

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

func lookupPrefixPos(s string, prefixes []string) int {
	sb := &strings.Builder{}

	for i := 0; i < len(s); i++ {
		sb.WriteByte(s[i])
		if s[i] != '0' {
			break
		}
	}

	return slices.Index(prefixes, sb.String())
}

type listingPrefixYielder struct {
	startPos int
	prefixes []string
}

func (l *listingPrefixYielder) Next() (string, error) {

	if l.startPos < len(l.prefixes) {
		s := l.prefixes[l.startPos]
		l.startPos++
		return s, nil
	}

	return "", io.EOF
}
