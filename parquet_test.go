package deltago

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/iter"
	"github.com/repeale/fp-go"
	"github.com/stretchr/testify/assert"
)

func TestLocalParquetReadWrite(t *testing.T) {

	dir, err := filepath.Abs("./")
	assert.NoError(t, err)

	w, err := newParquetActionWriter(Config{
		StorageConfig: StorageConfig{
			Scheme: Local,
			LogDir: dir,
		},
	})
	assert.NoError(t, err)

	path := "local-test.parquet"
	defer os.RemoveAll(path)

	err = w.Open(path, actionSchemaDefinitionString)
	assert.NoError(t, err)

	expected := []*action.SingleAction{
		{
			Txn: &action.SetTransaction{
				AppId:       "1",
				Version:     1,
				LastUpdated: util.PtrOf[int64](1),
			},
		},
		{
			Txn: &action.SetTransaction{
				AppId:   "1",
				Version: 1,
			},
		},
		{
			Txn: &action.SetTransaction{
				Version: 1,
			},
		},
		{
			Add: &action.AddFile{
				Path:       "1",
				DataChange: true,
				PartitionValues: map[string]string{
					"a": "b",
				},
				Size:             1,
				ModificationTime: 1,
				Stats:            "s",
				Tags:             map[string]string{"a": "f"},
			},
		},
		{
			Add: &action.AddFile{
				Path:       "1",
				DataChange: true,
				PartitionValues: map[string]string{
					"a": "b",
				},
				Size:             1,
				ModificationTime: 1,
			},
		},
		{
			Add: &action.AddFile{
				Path:             "1",
				DataChange:       true,
				Size:             1,
				ModificationTime: 1,
			},
		},
		{
			Remove: &action.RemoveFile{
				Path:                 "1",
				DataChange:           true,
				DeletionTimestamp:    util.PtrOf[int64](1),
				PartitionValues:      map[string]string{"a": "1"},
				ExtendedFileMetadata: true,
				Size:                 util.PtrOf[int64](1),
			},
		},
		{
			MetaData: &action.Metadata{
				ID:               "1",
				Name:             "string",
				Description:      "x",
				Format:           action.Format{Proviver: "string"},
				SchemaString:     "string",
				PartitionColumns: []string{"a", "b"},
				Configuration:    map[string]string{"a": "1"},
				CreatedTime:      util.PtrOf[int64](1),
			},
		},
		{
			MetaData: &action.Metadata{
				ID:           "1",
				Name:         "string",
				Description:  "x",
				Format:       action.Format{Proviver: "string"},
				SchemaString: "string",
				//PartitionColumns: []string{},
				Configuration: map[string]string{"a": "1"},
				CreatedTime:   util.PtrOf[int64](1),
			},
		},
		{
			MetaData: &action.Metadata{
				ID:           "1",
				Name:         "string",
				Description:  "x",
				Format:       action.Format{Proviver: "string"},
				SchemaString: "string",
				CreatedTime:  util.PtrOf[int64](1),
			},
		},
		{
			Protocol: &action.Protocol{
				MinReaderVersion: 1,
				MinWriterVersion: 2,
			},
		},
		{
			Cdc: &action.AddCDCFile{
				Path:            "1",
				PartitionValues: map[string]string{"a": "b"},
				Size:            1,
			},
		},
		{
			CommitInfo: &action.CommitInfo{
				Version:             util.PtrOf[int64](1),
				Timestamp:           1,
				UserID:              util.PtrOf("u1"),
				UserName:            util.PtrOf("u1"),
				Operation:           "1",
				OperationParameters: map[string]string{"a": "b"},
				Job: &action.JobInfo{
					JobID:       "1",
					JobName:     "string",
					RunId:       "string",
					JobOwnerId:  "string",
					TriggerType: "string",
				},
				Notebook: &action.NotebookInfo{
					NotebookId: "string",
				},
				ClusterId:        util.PtrOf("u1"),
				ReadVersion:      util.PtrOf[int64](1),
				IsolationLevel:   util.PtrOf("u1"),
				IsBlindAppend:    util.PtrOf(true),
				OperationMetrics: map[string]string{"a": "b"},
				UserMetadata:     util.PtrOf("u1"),
				EngineInfo:       util.PtrOf("u1"),
			},
		},
	}

	for _, ex := range expected {
		err = w.Write(ex)
		assert.NoError(t, err)
	}

	err = w.Close()
	assert.NoError(t, err)

	r, err := newCheckpointReader(fmt.Sprintf("file://%s", dir))
	assert.NoError(t, err)

	it, err := r.Read(path)
	assert.NoError(t, err)
	defer it.Close()

	s, err := iter.ToSlice(it)
	assert.NoError(t, err)
	actual := fp.Map(func(a action.Action) *action.SingleAction { return a.Wrap() })(s)

	assert.Equal(t, expected, actual)
}
