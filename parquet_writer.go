package deltago

import (
	"os"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	pq "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	parq "github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/rotisserie/eris"
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
}

func newParquetActionWriter(config *parquetActionWriterConfig) parquetActionWriter {
	if config.Local != nil {
		return &localParquetActionWriter{}
	}
	return nil
}

// localParquetActionWriter uses os.Rename to achive the atomic writes.
type localParquetActionWriter struct {
	name     string
	tempName string
	fw       *floor.Writer
}

func (l *localParquetActionWriter) Open(path string, schemaString string) error {

	if _, err := os.Stat(path); err == nil {
		return errno.FileAlreadyExists(path)
	}

	schema, err := parquetschema.ParseSchemaDefinition(schemaString)
	if err != nil {
		return eris.Wrap(err, "parsing schema definition")
	}

	tempFile, err := os.CreateTemp(os.TempDir(), l.name)
	if err != nil {
		return eris.Wrap(err, l.name)
	}

	l.name = path
	l.tempName = tempFile.Name()

	fw, err := floor.NewFileWriter(tempFile.Name(),
		pq.WithSchemaDefinition(schema),
		pq.WithCompressionCodec(parq.CompressionCodec_SNAPPY),
	)
	if err != nil {
		return err
	}
	l.fw = fw

	return nil
}

func (l *localParquetActionWriter) Write(a *action.SingleAction) error {
	if err := l.fw.Write(&actionMarshaller{a: a}); err != nil {
		return eris.Wrap(err, "local parquet writer writing")
	}
	return nil
}

func (l *localParquetActionWriter) Close() error {

	if err := l.fw.Close(); err != nil {
		return eris.Wrap(err, "close error")
	}
	if err := os.Rename(l.tempName, l.name); err != nil {
		return eris.Wrap(err, "rename error")
	}
	if err := os.RemoveAll(l.tempName); err != nil {
		return eris.Wrap(err, "remove error")
	}
	return nil
}
