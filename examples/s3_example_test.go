package examples

import (
	"log"
	"os"
	"testing"

	delta "github.com/csimplestring/delta-go"
)

func TestS3LogStoreExample(t *testing.T) {

	os.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	os.Setenv("AWS_DISABLE_SSL", "true")
	os.Setenv("AWS_S3_FORCE_PATH_STYLE", "true")
	os.Setenv("AWS_ACCESS_KEY_ID", "foo")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "bar")
	// for localstack test only
	os.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")

	path := "s3://golden/snapshot-data0/"
	config := delta.Config{
		StoreType: "s3",
	}

	table, err := delta.ForTable(path, config, &delta.SystemClock{})
	if err != nil {
		log.Fatal(err)
	}

	s, err := table.Snapshot()
	if err != nil {
		log.Fatal(err)
	}

	version := s.Version()
	log.Println(version)

	files, err := s.AllFiles()
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		log.Println(f.Path)
	}
}
