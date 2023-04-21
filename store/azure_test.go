package store

import (
	"os"
	"testing"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/iter"
	"github.com/rotisserie/eris"
	"github.com/stretchr/testify/assert"
)

func TestAzureStore_Read(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test.")
	}

	containerName := "golden"
	connStr := "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
	logPath := "checkpoint/_delta_log/"

	os.Setenv("AZURE_CONNECTION_STR", connStr)
	os.Setenv("AZURE_CONTAINER", containerName)

	s, err := newAzureStore(logPath)
	assert.NoError(t, err)

	// read last checkpoint
	it, err := s.Read("_last_checkpoint")
	assert.NoError(t, err)

	defer it.Close()
	res, err := iter.ToSlice(it)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res))

	// read not found
	_, err = s.Read("not_found")
	assert.True(t, eris.As(err, &errno.ErrFileNotFound))
}

func TestAzureStore_ListFrom(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test.")
	}

	containerName := "golden"
	connStr := "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
	logPath := "checkpoint/_delta_log/"

	os.Setenv("AZURE_CONNECTION_STR", connStr)
	os.Setenv("AZURE_CONTAINER", containerName)

	s, err := newAzureStore(logPath)
	assert.NoError(t, err)

	it, err := s.ListFrom("00000000000000000012.json")
	assert.NoError(t, err)
	defer it.Close()

	slice, err := iter.ToSlice(it)
	assert.NoError(t, err)
	assert.Equal(t, "checkpoint/_delta_log/00000000000000000012.json", slice[0].path)
	assert.Equal(t, "checkpoint/_delta_log/00000000000000000013.json", slice[1].path)
	assert.Equal(t, "checkpoint/_delta_log/00000000000000000014.json", slice[2].path)
}

func TestAzureStore_Write(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test.")
	}

	containerName := "golden"
	connStr := "AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
	logPath := "checkpoint/_delta_log/"

	os.Setenv("AZURE_CONNECTION_STR", connStr)
	os.Setenv("AZURE_CONTAINER", containerName)

	s, err := newAzureStore(logPath)
	assert.NoError(t, err)

	data := []string{
		"a\n",
		"bbbbbbbbbbbbbbbbb\n",
	}
	actions := iter.FromSlice(data)
	err = s.Write("test.json", actions, false)
	assert.NoError(t, err)

	it, err := s.Read("test.json")
	assert.NoError(t, err)
	res, err := iter.ToSlice(it)
	assert.NoError(t, err)
	assert.Equal(t, res, data)
}
