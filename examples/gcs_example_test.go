package examples

import (
	"log"
	"os"
	"testing"

	delta "github.com/csimplestring/delta-go"
)

func TestGCSLogStoreExample(t *testing.T) {

	// for local emulator only
	os.Setenv("STORAGE_EMULATOR_HOST", "localhost:4443")

	path := "gs://golden/snapshot-data0/"
	config := delta.Config{
		StoreType: "gs",
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
