package deltago

import (
	"context"
	"fmt"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	goparquet "github.com/fraugster/parquet-go"
	pq "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor/interfaces"
	parq "github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/rotisserie/eris"
	"gocloud.dev/blob"
)

type parquetActionWriter interface {
	Open(path string, schema string) error
	Write(a *action.SingleAction) error
	Close() error
}

type parquetActionWriterConfig struct {
	Local *parquetActionLocalWriterConfig
}

type parquetActionLocalWriterConfig struct {
	LogDir string
}

func newParquetActionWriter(config *parquetActionWriterConfig) (parquetActionWriter, error) {
	if config.Local != nil {
		url := fmt.Sprintf("file://%s?create_dir=true", config.Local.LogDir)
		bucket, err := blob.OpenBucket(context.Background(), url)
		if err != nil {
			return nil, err
		}
		return &localParquetActionWriter{
			bucket: bucket,
		}, nil
	}
	return nil, nil
}

// localParquetActionWriter uses os.Rename to achive the atomic writes.
type localParquetActionWriter struct {
	name   string
	bucket *blob.Bucket
	bw     *blob.Writer
	fw     *pq.FileWriter
}

func (l *localParquetActionWriter) Open(path string, schemaString string) error {

	exists, err := l.bucket.Exists(context.Background(), path)
	if err != nil {
		return err
	}
	if exists {
		return errno.FileAlreadyExists(path)
	}

	schema, err := parquetschema.ParseSchemaDefinition(schemaString)
	if err != nil {
		return eris.Wrap(err, "parsing schema definition")
	}

	bw, err := l.bucket.NewWriter(context.Background(), path, nil)
	if err != nil {
		return err
	}

	fw := goparquet.NewFileWriter(bw,
		goparquet.WithSchemaDefinition(schema),
		pq.WithCompressionCodec(parq.CompressionCodec_SNAPPY))

	l.name = path
	l.fw = fw
	l.bw = bw

	return nil
}

func (l *localParquetActionWriter) Write(a *action.SingleAction) error {
	obj := interfaces.NewMarshallObjectWithSchema(nil, l.fw.GetSchemaDefinition())
	am := &actionMarshaller{a: a}
	if err := am.MarshalParquet(obj); err != nil {
		return err
	}

	if err := l.fw.AddData(obj.GetData()); err != nil {
		return eris.Wrap(err, "local parquet writer writing")
	}
	return nil
}

func (l *localParquetActionWriter) Close() error {

	if err := l.fw.Close(); err != nil {
		return eris.Wrap(err, "parquet file writer close error")
	}
	if err := l.bw.Close(); err != nil {
		return eris.Wrap(err, "parquet blob writer close error")
	}

	return nil
}
