# Delta Go

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/csimplestring/delta-go/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/csimplestring/delta-go)](https://goreportcard.com/report/github.com/csimplestring/delta-go)

## About

This repository contains a Go package that provides a connector for [Delta Lake](https://delta.io/) - an open-source storage layer that brings ACID transactions to Apache Spark big data workloads. A Go portal of the official Scala [delta standalone](https://github.com/delta-io/connectors). 

What is it?

- It provides low level access to read and write Delta Lake **metadata** by implementing the Delta Lake transaction log protocol.

What is it not?

- It does not read, write or update the data for Delta Lake table directly, but the compute engine on top of it should do that.

## Supported Log Store

- [x] Local file system 
- [x] Azure Blob Storage 
- [x] Google Cloud Storage 
- [X] AWS S3 (single cluster)
- [ ] AWS S3 (multi clusters)

## Usage Example

```go
package examples

import (
	"log"
	"os"
	"testing"

	delta "github.com/csimplestring/delta-go"
)

func TestAzureBlobExample(t *testing.T) {

	// Go SDK uses the environment variables AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY,
	// and AZURE_STORAGE_SAS_TOKEN to configure the credentials. AZURE_STORAGE_ACCOUNT is required, along with one of the other two.

	// this is the default credentials for Azurite Local Emulator
	os.Setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
	os.Setenv("AZURE_STORAGE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

	// required the Azurite local emulator, for production, not needed
	os.Setenv("AZURE_STORAGE_DOMAIN", "localhost:10000")
	os.Setenv("AZURE_STORAGE_PROTOCOL", "http")
	os.Setenv("AZURE_STORAGE_IS_CDN", "false")
	os.Setenv("AZURE_STORAGE_IS_LOCAL_EMULATOR", "true")

	// golden is the container name
	// snapshot-data0/_delta_log/ will be the log directory
	// the golden table test data is used in this example
	dataPath := "azblob://golden/snapshot-data0/"
	config := delta.Config{
		StoreType: "azblob",
	}

	table, err := delta.ForTable(dataPath, config, &delta.SystemClock{})
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
```
More examples for different storage backend, see examples folder.

## Contributing

Contributions to this project are welcome. To contribute, please follow these steps:

1. Fork the repository
2. Create a new branch for your feature/bugfix
3. Make changes and commit them with clear commit messages
4. Push your changes to your forked repository
5. Create a pull request to the original repository

## License

This project is licensed under the [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0).
