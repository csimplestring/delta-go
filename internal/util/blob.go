package util

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"io"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	"gocloud.dev/gcerrors"
)

type BlobDir struct {
	bucket *blob.Bucket
	urlstr string
}

func NewBlobDir(urlstr string) (*BlobDir, error) {
	bucket, err := blob.OpenBucket(context.Background(), urlstr)
	if err != nil {
		return nil, err
	}

	return &BlobDir{
		bucket: bucket,
		urlstr: urlstr,
	}, nil
}

func (b *BlobDir) listingBlob(ctx context.Context, prefix string) ([]string, error) {
	iter := b.bucket.List(&blob.ListOptions{
		Prefix:    prefix,
		Delimiter: "/",
	})

	var res []string
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if obj.IsDir {
			if ret, err := b.listingBlob(ctx, obj.Key); err != nil {
				return nil, err
			} else {
				res = append(res, ret...)
			}
		} else {
			res = append(res, obj.Key)
		}
	}

	return res, nil
}

func (b *BlobDir) Close() error {
	return b.bucket.Close()
}

func (b *BlobDir) Copy(srcPrefix string) (string, []string, error) {

	ctx := context.Background()

	dir := fmt.Sprintf("temp-%s-xxx-%d", srcPrefix, time.Now().Unix())

	blobs, err := b.listingBlob(ctx, srcPrefix)
	if err != nil {
		return "", nil, err
	}

	var tempBlobs []string
	for _, srcKey := range blobs {
		dstKey := dir + "-" + srcKey
		err = b.bucket.Copy(ctx, dstKey, srcKey, nil)
		if err != nil {
			return "", nil, err
		}
		tempBlobs = append(tempBlobs, dstKey)
	}

	return fmt.Sprintf("%s-%s", dir, srcPrefix), tempBlobs, nil
}

func (b *BlobDir) Delete(dir string, files []string, hardDelete bool) error {

	if strings.HasPrefix(b.urlstr, "file://") {
		p, err := url.Parse(b.urlstr)
		if err != nil {
			return err
		}
		fullDir := p.Path + "/" + dir
		return os.RemoveAll(fullDir)
	}

	if hardDelete {
		ctx := context.Background()
		for _, f := range files {
			if err := b.bucket.Delete(ctx, f); err != nil {
				if gcerrors.NotFound == gcerrors.Code(err) {
					continue
				} else {
					return err
				}
			}
		}
	}

	return nil
}

func (b *BlobDir) DeleteFile(file string) error {
	return b.bucket.Delete(context.Background(), file)
}

func (b *BlobDir) CreateTemp() (dir string, placeHolder string, err error) {

	dir = fmt.Sprintf("temp-%d/", time.Now().Unix())
	placeHolder = dir + ".xxx"
	err = b.bucket.WriteAll(context.Background(), placeHolder, []byte{}, nil)
	if err != nil {
		return
	}

	return
}
