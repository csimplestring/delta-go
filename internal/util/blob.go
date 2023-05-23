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
)

func listingBlob(ctx context.Context, b *blob.Bucket, prefix string) ([]string, error) {
	iter := b.List(&blob.ListOptions{
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
			if ret, err := listingBlob(ctx, b, obj.Key); err != nil {
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

func CopyBlobDir(urlstr string, prefix string) (string, error) {

	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, urlstr)
	if err != nil {
		return "", err
	}
	defer b.Close()

	dir := fmt.Sprintf("temp-%s-xxx-%d", prefix, time.Now().Unix())

	blobs, err := listingBlob(ctx, b, prefix)
	if err != nil {
		return "", err
	}
	for _, srcKey := range blobs {
		dstKey := dir + "-" + srcKey
		err = b.Copy(ctx, dstKey, srcKey, nil)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%s-%s", dir, prefix), nil
}

func DelBlobFiles(urlstr string, dir string) error {
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, urlstr)
	if err != nil {
		return err
	}
	defer b.Close()

	// for other cloud storages, we do not delete those temporary files
	if strings.HasPrefix(urlstr, "file://") {
		p, err := url.Parse(urlstr)
		if err != nil {
			return err
		}
		fullDir := p.Path + "/" + dir
		return os.RemoveAll(fullDir)
	}

	return nil
}

func CreateDir(urlstr string) (string, error) {
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, urlstr)
	if err != nil {
		return "", err
	}

	dir := fmt.Sprintf("temp-%d/", time.Now().Unix())
	key := dir + ".xxx"
	err = b.WriteAll(context.Background(), key, []byte{}, nil)
	if err != nil {
		return "", err
	}

	return dir, nil
}
