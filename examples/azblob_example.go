package examples

import (
	"log"

	delta "github.com/csimplestring/delta-go"
)

func main() {

	// Azure Blob Storage URLs in the Go CDK allow you to identify Azure Blob Storage containers
	// when opening a bucket with blob.OpenBucket.
	// Go CDK uses the environment variables AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY,
	// and AZURE_STORAGE_SAS_TOKEN to configure the credentials. AZURE_STORAGE_ACCOUNT is required, along with one of the other two.

	path := "azblob://mycontainer"
	config := delta.Config{
		StoreType: "azblob",
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
