package util

import (
	"context"
	"fmt"
	"strings"

	"io"
	"math/rand"

	"gocloud.dev/blob"
)

func CopyBlobDir(urlstr string, prefix string) (string, error) {

	ctx := context.Background()

	b, err := blob.OpenBucket(ctx, urlstr)
	if err != nil {
		return "", err
	}

	dir := fmt.Sprintf("temp-test-xxx-%d", rand.Int())
	iter := b.List(&blob.ListOptions{
		Prefix: prefix,
	})
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}

		if strings.HasSuffix(obj.Key, "/") {
			continue
		}

		dstKey := dir + "-" + obj.Key

		err = b.Copy(ctx, dstKey, obj.Key, nil)
		if err != nil {
			return "", err
		}
	}

	return dir + "-" + prefix, nil
}

func DelBlobFiles(urlstr string, dir string) error {
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, urlstr)
	if err != nil {
		return err
	}

	iter := b.List(&blob.ListOptions{
		Prefix: dir,
	})
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := b.Delete(ctx, obj.Key); err != nil {
			return err
		}
	}

	if strings.HasPrefix(urlstr, "file://") {
		if err := b.Delete(ctx, dir+"/_delta_log"); err != nil {
			return err
		}
		if err := b.Delete(ctx, dir); err != nil {
			return err
		}
	}

	return nil
}

func CreateDir(urlstr string) (string, error) {
	ctx := context.Background()
	b, err := blob.OpenBucket(ctx, urlstr)
	if err != nil {
		return "", err
	}

	dir := fmt.Sprintf("temp-%d", rand.Int())
	err = b.WriteAll(context.Background(), dir, []byte{}, nil)
	if err != nil {
		return "", err
	}

	return dir, nil
}
