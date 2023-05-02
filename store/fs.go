package store

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/csimplestring/delta-go/errno"
	iter "github.com/csimplestring/delta-go/iter_v2"
	"github.com/rotisserie/eris"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
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

var _ FS = &LocalFS{}

type FS interface {
	Exists(path string) (bool, error)
	Mkdirs(path string) error
	Create(path string, overwrite bool) error
}

type LocalFS struct {
}

func (l *LocalFS) Mkdirs(path string) error {
	path = strings.TrimSuffix(path, "/") + "/"

	_, err := blob.OpenBucket(context.Background(), path+"?create_dir=true")
	return err
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

type AzureBlobFS struct {
}

func (a *AzureBlobFS) Exists(path string) (bool, error) {
	panic("not implemented") // TODO: Implement
}

func (a *AzureBlobFS) Mkdirs(path string) error {
	bucket, err := blob.OpenBucket(context.Background(), path)
	if err != nil {
		return err
	}
	defer bucket.Close()

	path = strings.TrimPrefix(path, "azblob://")
	path = strings.TrimSuffix(path, "/") + "/"

	return bucket.WriteAll(context.Background(), path, nil, nil)

}

func (a *AzureBlobFS) Create(path string, overwrite bool) error {
	panic("not implemented") // TODO: Implement
}

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

func newAzureBlobStore(container string, logDir string, localemu bool) (*baseStore, error) {
	var url string
	if localemu {
		url = fmt.Sprintf("azblob://%s?localemu=true&domain=localhost:10000&protocol=http&prefix=%s", container, logDir)
	} else {
		url = fmt.Sprintf("azblob://%s?prefix=%s", container, logDir)
	}

	bucket, err := blob.OpenBucket(context.Background(), url)
	if err != nil {
		return nil, err
	}
	return &baseStore{
		logDir: logDir,
		bucket: bucket,
	}, nil
}

type baseStore struct {
	logDir string
	bucket *blob.Bucket
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
	startPos := lookupPrefixPos(startPath, prefilledListingPrefixes)
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
	panic("not implemented") // TODO: Implement
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
