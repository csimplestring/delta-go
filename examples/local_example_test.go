package examples

import (
	"log"
	"path/filepath"
	"testing"

	delta "github.com/csimplestring/delta-go"
)

func TestLocalExample(t *testing.T) {
	path, err := filepath.Abs("../tests/golden/snapshot-data0")
	if err != nil {
		log.Fatal(err)
	}

	path = "file://" + path + "/"

	config := delta.Config{
		StorageConfig: delta.StorageConfig{
			Scheme: delta.Local,
		},
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