package store

import "github.com/csimplestring/delta-go/iter"

type AzureStore struct {
	LogPath string
}

func (a *AzureStore) Root() string {
	panic("not implemented") // TODO: Implement
}

// Read the given file and return an `Iterator` of lines, with line breaks removed from
// each line. Callers of this function are responsible to close the iterator if they are
// done with it.
func (a *AzureStore) Read(path string) (iter.Iter[string], error) {
	panic("not implemented") // TODO: Implement
}

// List the paths in the same directory that are lexicographically greater or equal to (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
func (a *AzureStore) ListFrom(path string) (iter.Iter[*FileMeta], error) {
	panic("not implemented") // TODO: Implement
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
	panic("not implemented") // TODO: Implement
}

// Whether a partial write is visible for the underlying file system of `path`.
func (a *AzureStore) IsPartialWriteVisible(path string) bool {
	panic("not implemented") // TODO: Implement
}
