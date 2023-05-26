# Delta Go

## About

This repository contains a Go package that provides a connector for [Delta Lake](https://delta.io/) - an open-source storage layer that brings ACID transactions to Apache Spark big data workloads. A Go portal of the official Scala [delta standalone](https://github.com/delta-io/connectors). 

What is it?

- It provides low level access to read and write Delta Lake **metadata** by implementing the Delta Lake transaction log protocol.

What is it not?

- It does not read, write or update the data for Delta Lake table directly, but the compute engine on top of it should do that.

## Supported backends

- Local file system (Done)
- Azure Blob Storage (Done)

## Status

- Currently only the local file system is fully supported and thoroughly tested against golden table data in the official Delta Standalone repo.
- For other cloud storage, on the roadmap, any contribution is welcome!

## Usage

```go
package examples

import (
	"log"
	"path/filepath"
	"testing"

	delta "github.com/csimplestring/delta-go"
)

func main() {
	path = "file://YOUR_DELTA_LOG_FOLDER/"

	config := delta.Config{
		StorageConfig: delta.StorageConfig{
			Scheme: delta.Local,
		},
	}

	table, err := delta.ForTable(path, config, &delta.SystemClock{})
	if err != nil {
		log.Fatal(err)
	}

	// get the snapshot 
	s, err := table.Snapshot()
	if err != nil {
		log.Fatal(err)
	}
	
	// get the log version
	version := s.Version()
	log.Println(version)

	// iterate all the log files
	files, err := s.AllFiles()
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		log.Println(f.Path)
	}
}

```

## Contributing

Contributions to this project are welcome. To contribute, please follow these steps:

1. Fork the repository
2. Create a new branch for your feature/bugfix
3. Make changes and commit them with clear commit messages
4. Push your changes to your forked repository
5. Create a pull request to the original repository

## License

This project is licensed under the [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0).