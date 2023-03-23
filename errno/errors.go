package errno

import (
	"errors"
	"fmt"

	"github.com/rotisserie/eris"
)

var ErrIllegalState = errors.New("illegal state")
var ErrIllegalArgument = errors.New("illegal argument")
var ErrUnexpectedFileType = errors.New("unexpected file type")
var ErrNullPointer = errors.New("null pointer")
var ErrClassCast = errors.New("class cast type failed")
var ErrAssertion = errors.New("assertion failed")
var ErrUnsupportedOperation = errors.New("unsupported operation")
var ErrDeltaStandalone = &DeltaStandaloneError{Msg: "delta standalone error"}
var ErrFileAlreadyExists = errors.New("file already exists")
var ErrFileNotFound = errors.New("file not found")
var ErrConcurrentModification = errors.New("concurrent modification")
var ErrJSONUnmarshal = errors.New("json unmarshal error")
var ErrJSONMarshal = errors.New("json marshal error")

func ActionNotFound(action string, version int64) error {
	return eris.Wrap(ErrIllegalState,
		fmt.Sprintf("The %s of your Delta table couldn't be recovered while Reconstructing "+
			"version: %d. Did you manually delete files in the _delta_log directory?", action, version))
}

func UnsupportedFileSystem(msg string) error {
	return eris.Wrap(ErrIllegalArgument, msg)
}

func JsonUnmarshalError(err error) error {
	return eris.Wrap(ErrJSONUnmarshal, err.Error())
}

func JsonMarshalError(err error) error {
	return eris.Wrap(ErrJSONMarshal, err.Error())
}

func MetadataChangedError() error {
	return eris.Wrap(ErrConcurrentModification, "metadata changed")
}

func ProtocolChangedException(msg string) error {
	return eris.Wrap(ErrConcurrentModification, msg)
}

func InvalidProtocolVersionError() error {
	return eris.New("invalid protocol version")
}

func IllegalStateError(msg string) error {
	return eris.Wrap(ErrIllegalState, msg)
}

func MaxCommitRetriesExceededError(msg string) error {
	return eris.Wrap(ErrIllegalState, msg)
}

func MetadataAbsentError() error {
	return eris.Wrap(ErrIllegalState, "Couldn't find Metadata while committing the first version of the Delta table.")
}

func AddFilePartitioningMismatchError() error {
	return eris.Wrap(ErrIllegalState, "The AddFile contains partitioning schema different from the table's partitioning schema")
}

func ModifyAppendOnlyTableError() error {
	return eris.Wrap(ErrUnsupportedOperation,
		"This table is configured to only allow appends. If you would like to permit "+
			"updates or deletes, use 'ALTER TABLE <table_name> SET TBLPROPERTIES "+
			"(appendOnly=false)'.")
}

func SchemaChangeError(oldSchema string, newSchema string) error {
	return eris.Wrap(ErrIllegalArgument, fmt.Sprintf(`
	"""Detected incompatible schema change:
	|	old schema: %s
	|
	|	new schema: %s
	`, oldSchema, newSchema))
}

func NonPartitionColumnAbsentError() error {
	return eris.Wrap(ErrDeltaStandalone, "Data written into Delta needs to contain at least one "+
		"non-partitioned column")
}

func PartitionColumnsNotFoundError(partCols []string, schema string) error {
	return eris.Wrap(ErrDeltaStandalone,
		fmt.Sprintf("Partition columns %s not found iin schema %s", partCols, schema))
}

func AssertionError(msg string) error {
	return eris.Wrap(ErrAssertion, msg)
}

func InvalidPartitionColumn(err error) error {
	dse := &DeltaStandaloneError{
		Msg: `Found partition columns having invalid character(s) among " ,;{}()\n\t=". Please """ +
			"change the name to your partition columns. `,
	}
	return eris.Wrap(dse, err.Error())
}

func NestedNotNullConstraintError(parent string, nested string, nestType string) error {
	return &DeltaStandaloneError{
		Msg: fmt.Sprintf(
			"The %s type of the field %s contains a NOT NULL "+
				"constraint. Delta does not support NOT NULL constraints nested within arrays or maps. "+
				"Parsed $nestType type:\n %s",
			nestType, parent, nested),
	}
}

func FieldTypeMismatch(fieldName string, actualType string, desiredType string) error {
	return eris.Wrap(ErrClassCast, fmt.Sprintf("The data type of field %s is %s, Cannot cast it to %s", fieldName, actualType, desiredType))
}

func EmptyDirectoryError(path string) error {
	return eris.Wrap(ErrFileNotFound, "no files found in the log dir "+path)
}

func UnexpectedFileType(path string) error {
	return eris.Wrap(ErrUnexpectedFileType, path)
}

func DeltaVersionNotContinuous(versions []int64) error {
	return eris.Wrap(ErrIllegalState, fmt.Sprintf("Versions (%v) are not contiguous", versions))
}

func NoFirstDeltaFile() error {
	return eris.Wrap(ErrIllegalArgument, "did not get the first delta to compute snapshot")
}

func NoLastDeltaFile() error {
	return eris.Wrap(ErrIllegalArgument, "did not get the last delta to compute snapshot")
}

func MissingPartFile(version int64) error {
	return eris.Wrap(ErrIllegalState, fmt.Sprintf("Couldn't find all part files of the checkpoint version: %d", version))
}

func NoReproducibleHistoryFound(path string) error {
	return eris.Wrap(ErrDeltaStandalone, "no reproducible commit found in "+path)
}

func NoHistoryFound(path string) error {
	return eris.Wrap(ErrDeltaStandalone, "no commit found in "+path)
}

func VersionNotExist(userVersion int64, earliest int64, latest int64) error {
	return eris.Wrap(ErrDeltaStandalone,
		fmt.Sprintf("Cannot time travel Delta table to version %d, Available versions [%d, %d]", userVersion, earliest, latest))
}

func TimestampEarlierThanTableFirstCommit(userTimestamp int64, commitTs int64) error {
	return eris.Wrap(ErrIllegalArgument, fmt.Sprintf("The provided timestamp %d is before the earliest version available to this table (%d). Please use a timestamp greater than or equal to %d.", userTimestamp, commitTs, commitTs))
}

func TimestampLaterThanTableLastCommit(userTimestamp int64, commitTs int64) error {
	return eris.Wrap(ErrIllegalArgument, fmt.Sprintf("The provided timestamp %d is before the latest version available to this table (%d). Please use a timestamp less than or equal to %d.", userTimestamp, commitTs, commitTs))
}

type ConcurrentModificationError struct {
	Msg string
}

func (d *ConcurrentModificationError) Error() string {
	return d.Msg
}

func ConcurrentDeleteDelete(file string) error {
	msg := fmt.Sprintf("This transaction attempted to delete one or more files that were deleted "+
		"(for example %s) by a concurrent update. Please try the operation again.", file)
	return eris.Wrap(ErrConcurrentModification, msg)
}

func ConcurrentDeleteRead(file string) error {
	msg := fmt.Sprintf("This transaction attempted to read one or more files that were deleted"+
		" (for example %s) by a concurrent update. Please try the operation again.", file)
	return eris.Wrap(ErrConcurrentModification, msg)
}

func ConcurrentAppend(partition string) error {
	msg := fmt.Sprintf("Files were added to %s by a concurrent update. "+
		"Please try the operation again.", partition)
	return eris.Wrap(ErrConcurrentModification, msg)
}

func ConcurrentTransaction() error {
	msg := "This error occurs when multiple streaming queries are using the same checkpoint to write " +
		"into this table. Did you run multiple instances of the same streaming query" +
		" at the same time?"
	return eris.Wrap(ErrConcurrentModification, msg)
}

// ConcurrentTransactionError Thrown when concurrent transaction both attempt to update the same idempotent transaction.
type ConcurrentTransactionError struct {
	Msg string
}

func (c *ConcurrentTransactionError) Error() string {
	return c.Msg
}

// DeltaStandaloneError Thrown when a query fails, usually because the query itself is invalid.
type DeltaStandaloneError struct {
	Msg string
}

func (c *DeltaStandaloneError) Error() string {
	return c.Msg
}

// MetadataChangeError Thrown when the metadata of the Delta table has changed between the time of read and the time of commit.
type MetadataChangeError struct {
	Msg string
}

func (c *MetadataChangeError) Error() string {
	return c.Msg
}

type IllegalArgError struct {
	Msg string
}

func (i *IllegalArgError) Error() string {
	return i.Msg
}

func FileAlreadyExists(file string) error {
	return eris.Wrap(ErrFileAlreadyExists, file)
}

func NullValueFoundForPrimitiveTypes(name string) error {
	return eris.Wrap(ErrNullPointer, fmt.Sprintf("Read a null value for field %s which is a primitive type.", name))
}

func NullValueFoundForNonNullSchemaField(name string, schema string) error {
	return eris.Wrap(ErrNullPointer, fmt.Sprintf("Read a null value for field %s which is a non-null type. yet schema indicates that this field can't be null. Schema: %s", name, schema))
}

func FileNotFound(msg string) error {
	return eris.Wrap(ErrFileNotFound, msg)
}
