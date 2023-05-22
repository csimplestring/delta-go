package deltago

import (
	"context"
	"fmt"
	"log"

	"io"
	"math/rand"

	"gocloud.dev/blob"
)

type DataTestUtil struct {
}

func CopyDir(urlstr string, prefix string) (string, error) {

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

		dstKey := dir + "-" + obj.Key

		log.Println("copying from " + obj.Key + " to " + dstKey)
		err = b.Copy(ctx, dstKey, obj.Key, nil)
		if err != nil {
			return "", err
		}
	}

	return dir + "-" + prefix, nil
}

func DelFiles(urlstr string, dir string) error {
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

		log.Println("deleting " + obj.Key)
		if err := b.Delete(ctx, obj.Key); err != nil {
			return err
		}
	}

	log.Println("deleting " + dir + "/_delta_log")
	if err := b.Delete(ctx, dir+"/_delta_log"); err != nil {
		return err
	}
	log.Println("deleting " + dir)
	if err := b.Delete(ctx, dir); err != nil {
		return err
	}

	return nil
}
