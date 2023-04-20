package store

import (
	"context"
	"os"
	"sort"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	deltaErrors "github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/iter"
	"github.com/rotisserie/eris"
)

type AzureStore struct {
	LogPath     string
	azContainer string
	azClient    *azblob.Client
}

func newAzureStore(path string) (*AzureStore, error) {
	connStr := os.Getenv("AZURE_CONNECTION_STR")
	if len(connStr) == 0 {
		return nil, eris.New("No AZURE_CONNECTION_STR found")
	}
	container := os.Getenv("AZURE_CONTAINER")
	if len(container) == 0 {
		return nil, eris.New("No AZURE_CONTAINER found")
	}

	client, err := azblob.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return nil, eris.Wrap(err, "failed to create azure storage client")
	}

	return &AzureStore{
		LogPath:     path,
		azClient:    client,
		azContainer: container,
	}, nil
}

func (a *AzureStore) Root() string {
	return a.LogPath
}

// Read the given file and return an `Iterator` of lines, with line breaks removed from
// each line. Callers of this function are responsible to close the iterator if they are
// done with it.
func (a *AzureStore) Read(path string) (iter.Iter[string], error) {
	path, _ = a.ResolvePathOnPhysicalStore(path)
	// assume the path is relative
	blobName := a.LogPath + path
	s, err := a.azClient.DownloadStream(context.Background(), a.azContainer, blobName, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, deltaErrors.FileNotFound(blobName)
		}
		return nil, eris.Wrap(err, "azure store read "+path)
	}

	return iter.FromReadCloser(s.Body), nil
}

// List the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
func (a *AzureStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {
	path, _ = a.ResolvePathOnPhysicalStore(path)
	// assume the path is relative
	blobName := a.LogPath + path

	pager := a.azClient.NewListBlobsFlatPager(a.azContainer, &container.ListBlobsFlatOptions{
		Prefix: &a.LogPath,
	})

	// TODO: avoid listing all the files
	var res []*FileMeta
	for pager.More() {
		resp, err := pager.NextPage(context.Background())

		if err != nil {
			if bloberror.HasCode(err, bloberror.BlobNotFound) {
				return nil, deltaErrors.FileNotFound(blobName)
			}
			return nil, eris.Wrap(err, "azure store listing "+path)
		}
		for _, item := range resp.Segment.BlobItems {
			if strings.Compare(blobName, *item.Name) <= 0 {
				res = append(res, &FileMeta{
					path:         *item.Name,
					timeModified: *item.Properties.LastModified,
					size:         uint64(*item.Properties.ContentLength),
				})
			}
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].path < res[j].path
	})

	// the path is the full path
	return iter.FromSlice(res), nil
}

// Write the given `actions` to the given `path` with or without overwrite as indicated.
// Implementation must throw FileAlreadyExistsException exception if the file already
// exists and overwrite = false. Furthermore, if isPartialWriteVisible returns false,
// implementation must ensure that the entire file is made visible atomically, that is,
// it should not generate partial files.
func (a *AzureStore) Write(path string, actions iter.Iter[string], overwrite bool) error {
	panic("not implemented") // TODO: Implement
}

// Resolve the fully qualified path for the given `path`.
func (a *AzureStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	// assume relative path
	path = strings.TrimPrefix(path, a.LogPath)

	return path, nil
}

// Whether a partial write is visible for the underlying file system of `path`.
func (a *AzureStore) IsPartialWriteVisible(path string) bool {
	panic("not implemented") // TODO: Implement
}
