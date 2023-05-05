package deltago

import (
	"context"
	"fmt"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/iter"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/rotisserie/eris"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

type checkpointReader interface {
	Read(path string) (iter.Iter[action.Action], error)
}

func newCheckpointReader(config Config) (checkpointReader, error) {
	if config.StorageConfig.Scheme == Local {
		url := fmt.Sprintf("file://%s?create_dir=true", config.StorageConfig.LogDir)
		bucket, err := blob.OpenBucket(context.Background(), url)
		if err != nil {
			return nil, err
		}

		return &localCheckpointReader{
			bucket: bucket,
		}, nil
	}

	return nil, eris.Wrap(errno.ErrIllegalArgument, "unsupported storage scheme "+string(config.StorageConfig.Scheme))
}

// LocalCheckpointReader implements checkpoint reader
type localCheckpointReader struct {
	bucket *blob.Bucket
}

func (l *localCheckpointReader) Read(path string) (iter.Iter[action.Action], error) {

	r, err := l.bucket.NewReader(context.Background(), path, nil)
	if err != nil {
		return nil, eris.Wrap(err, "")
	}

	fr, err := goparquet.NewFileReader(r)
	if err != nil {
		return nil, err
	}

	return &localParquetIterater{
		br:     r,
		reader: fr,
	}, nil
}

type localParquetIterater struct {
	br     *blob.Reader
	reader *goparquet.FileReader
}

func (p *localParquetIterater) Next() (action.Action, error) {
	data, err := p.reader.NextRow()
	if err != nil {
		return nil, err
	}

	obj := interfaces.NewUnmarshallObject(data)

	am := &actionMarshaller{a: &action.SingleAction{}}
	if err := am.UnmarshalParquet(obj); err != nil {
		return nil, eris.Wrap(err, "failed to read value")
	}

	return am.a.Unwrap(), nil
}

func (p *localParquetIterater) Close() error {
	return p.br.Close()
}

// SchemaDefinition
const actionSchemaDefinitionString = `
message SingleAction {
	optional group txn {
	  optional binary appId (STRING);
	  required int64 version ;
	  optional int64 lastUpdated ;
	}
	optional group add {
	  required binary path (STRING);
	  required int64 size ;
	  required group partitionValues (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }

	  required int64 modificationTime ;
	  required boolean dataChange;
	  optional binary stats (STRING);
	  optional group tags (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }
	}
	optional group remove {
	  required binary path (STRING);
	  required int64 deletionTimestamp ;
	  required boolean dataChange;
	  required boolean extendedFileMetadata;
	  optional group partitionValues (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }
	  optional int64 size ;
	  optional group tags (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }
	}
	optional group metaData {
	  optional binary id (STRING);
	  optional binary name (STRING);
	  optional binary description (STRING);
	  required group format {
		optional binary provider (STRING);
		optional group options (MAP) {
		  repeated group key_value {
			required binary key (STRING);
			optional binary value (STRING);
		  }
		}
	  }
	  optional binary schemaString (STRING);
	  optional group partitionColumns (LIST) {
		repeated group list {
		  optional binary element (STRING);
		}
	  }
	  optional group configuration (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }
	  optional int64 createdTime ;
	}
	optional group protocol {
	  required int32 minReaderVersion;
	  required int32 minWriterVersion;
	}
	optional group cdc {
	  required binary path (STRING);
	  required group partitionValues (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }
	  required int64 size ;
	  optional group tags (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }
	}
	optional group commitInfo {
	  optional int64 version ;
	  optional int64 timestamp;
	  optional binary userId (STRING);
	  optional binary userName (STRING);
	  optional binary operation (STRING);
	  optional group operationParameters (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }
	  optional group job {
		optional binary jobId (STRING);
		optional binary jobName (STRING);
		optional binary runId (STRING);
		optional binary jobOwnerId (STRING);
		optional binary triggerType (STRING);
	  }
	  optional group notebook {
		optional binary notebookId (STRING);
	  }
	  optional binary clusterId (STRING);
	  optional int64 readVersion ;
	  optional binary isolationLevel (STRING);
	  optional boolean isBlindAppend;
	  optional group operationMetrics (MAP) {
		repeated group key_value {
		  required binary key (STRING);
		  optional binary value (STRING);
		}
	  }
	  optional binary userMetadata (STRING);
	  optional binary engineInfo (STRING);
	}
  }
`
