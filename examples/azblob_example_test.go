package examples

import (
	"log"
	"os"
	"testing"

	delta "github.com/csimplestring/delta-go"
)

func TestAzureBlobExample(t *testing.T) {

	// Azure Blob Storage URLs in the Go CDK allow you to identify Azure Blob Storage containers
	// when opening a bucket with blob.OpenBucket.
	// Go CDK uses the environment variables AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY,
	// and AZURE_STORAGE_SAS_TOKEN to configure the credentials. AZURE_STORAGE_ACCOUNT is required, along with one of the other two.

	os.Setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
	os.Setenv("AZURE_STORAGE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

	// running the Azurite local emulator
	os.Setenv("AZURE_STORAGE_DOMAIN", "localhost:10000")
	os.Setenv("AZURE_STORAGE_PROTOCOL", "http")
	os.Setenv("AZURE_STORAGE_IS_CDN", "false")
	os.Setenv("AZURE_STORAGE_IS_LOCAL_EMULATOR", "true")

	path := "azblob://golden/snapshot-data0/"
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
