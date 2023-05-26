package deltago

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/csimplestring/delta-go/internal/util/path"
	"github.com/csimplestring/delta-go/isolation"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/op"
	"github.com/csimplestring/delta-go/store"
	"github.com/csimplestring/delta-go/types"
	"github.com/rotisserie/eris"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/repeale/fp-go"
	"github.com/samber/mo"
	"github.com/ulule/deepcopier"
)

// OptimisticTransaction  to perform a set of reads in a transaction and
// then commit a set of updates to the state of the log.
// All reads from the DeltaLog MUST go through this instance rather than directly to the DeltaLog otherwise they will not be checked for logical conflicts with concurrent updates.
// This class is not thread-safe.
type OptimisticTransaction interface {

	// Commit Modifies the state of the log by adding a new commit that is based on a read at the table's latest version as of this transaction's instantiation.
	// In the case of a conflict with a concurrent writer this method will throw an error.
	// Note: any AddFile with an absolute path within the table path will be updated to have a relative path (based off of the table path).
	// Because of this, be sure to generate all RemoveFiles using AddFiles read from the Delta Log (do not use the actions.AddFiles created pre-commit.)
	Commit(actions iter.Iter[action.Action], op *op.Operation, engineInfo string) (CommitResult, error)

	// Mark files matched by the readPredicate as read by this transaction.
	// Please note filtering is only supported on partition columns, thus the files matched may be a superset of the files in the Delta table that satisfy readPredicate.
	// Users should use Scan.ResidualPredicate() to check for any unapplied portion of the input predicate.
	// Internally, readPredicate and the matched readFiles will be used to determine if logical conflicts between this transaction and previously-committed transactions can be resolved (i.e. no error thrown).
	// For example:
	// - This transaction TXN1 reads partition 'date=2021-09-08' to perform an UPDATE and tries to commit at the next table version N.
	// - After TXN1 starts, another transaction TXN2 reads partition 'date=2021-09-07' and commits first at table version N (with no other metadata changes).
	// - TXN1 sees that another commit won, and needs to know whether to commit at version N+1 or fail.
	//   Using the readPredicates and resultant readFiles, TXN1 can see that none of its read files were changed by TXN2. Thus there are no logical conflicts and TXN1 can commit at table version N+1.
	MarkFilesAsRead(readPredicate types.Expression) (Scan, error)

	// Records an update to the metadata that should be committed with this transaction.
	// IMPORTANT: It is the responsibility of the caller to ensure that files currently present in the table are still valid under the new meta
	UpdateMetadata(metadata *action.Metadata) error

	// the metadata for this transaction.
	// It refers to the metadata of the table's latest version as of this transaction's instantiation unless updated during the transaction.
	Metadata() (*action.Metadata, error)

	// Mark the entire table as tainted (i.e. read) by this transaction.
	ReadWholeTable() error

	// returns the latest version that has committed for the idempotent transaction with given id.
	TxnVersion(id string) (int64, error)
}

const DELTA_MAX_RETRY_COMMIT_ATTEMPTS = 10000000

type CommitResult struct {
	Version int64
}

type optimisticTransactionImp struct {
	snapshot        *snapshotImp
	snapshotManager *SnapshotReader

	txnID                  uuid.UUID
	readTxn                []string
	readPredicates         []types.Expression
	readFiles              mapset.Set[*action.AddFile]
	readTheWholeTable      bool // false by default
	committed              bool //  false
	newMetadata            mo.Option[*action.Metadata]
	newProtocol            mo.Option[*action.Protocol]
	isCreatingNewTable     bool // false
	commitAttemptStartTime int64
	clock                  Clock

	configurations tableConfigurations
	lock           *sync.Mutex
	logStore       store.Store
	logPath        string
}

func newOptimisticTransaction(snapshot *snapshotImp,
	snapshotManager *SnapshotReader,
	clock Clock,
	configuration tableConfigurations,
	lock *sync.Mutex,
	logStore store.Store,
	logPath string) *optimisticTransactionImp {

	return &optimisticTransactionImp{
		snapshot:               snapshot,
		snapshotManager:        snapshotManager,
		txnID:                  uuid.New(),
		readTxn:                make([]string, 0),
		readPredicates:         make([]types.Expression, 0),
		readFiles:              mapset.NewSet[*action.AddFile](),
		readTheWholeTable:      false,
		committed:              false,
		newMetadata:            mo.None[*action.Metadata](),
		newProtocol:            mo.None[*action.Protocol](),
		isCreatingNewTable:     false,
		commitAttemptStartTime: 0,

		clock:          clock,
		configurations: configuration,
		lock:           lock,
		logStore:       logStore,
		logPath:        logPath,
	}
}

func (trx *optimisticTransactionImp) protocol() (*action.Protocol, error) {
	if trx.newProtocol.IsPresent() {
		return trx.newProtocol.MustGet(), nil
	}
	t, err := trx.snapshot.protocolAndMetadata.Get()
	if err != nil {
		return nil, err
	}
	return t.V1, nil
}

func (trx *optimisticTransactionImp) readVersion() int64 {
	return trx.snapshot.Version()
}

// Commit Modifies the state of the log by adding a new commit that is based on a read at the table's latest version as of this transaction's instantiation.
// In the case of a conflict with a concurrent writer this method will throw an error.
// Note: any AddFile with an absolute path within the table path will be updated to have a relative path (based off of the table path).
// Because of this, be sure to generate all RemoveFiles using AddFiles read from the Delta Log (do not use the actions.AddFiles created pre-commit.)
func (trx *optimisticTransactionImp) Commit(actionsIter iter.Iter[action.Action], op *op.Operation, engineInfo string) (CommitResult, error) {
	defer actionsIter.Close()

	var actions []action.Action
	var err error
	for a, err := actionsIter.Next(); err == nil; a, err = actionsIter.Next() {
		switch v := a.(type) {
		case *action.Metadata:
			trx.UpdateMetadata(v)
		default:
			actions = append(actions, v)
		}
	}
	if err != nil && err != io.EOF {
		return CommitResult{}, err
	}

	preparedActions, err := trx.prepareCommit(actions)
	if err != nil {
		return CommitResult{}, eris.Wrap(err, "prepareCommit")
	}

	var noDataChanged = true
	for _, a := range actions {
		switch v := a.(type) {
		case action.FileAction:
			noDataChanged = noDataChanged && (!v.IsDataChanged())
		}
	}

	var isolationLevelToUse isolation.Level
	if noDataChanged {
		isolationLevelToUse = isolation.Snapshot
	} else {
		isolationLevelToUse = isolation.Serializable
	}

	dependsOnFiles := len(trx.readPredicates) != 0 || trx.readFiles.Cardinality() != 0
	onlyAddFiles := true
	for _, a := range preparedActions {
		switch v := a.(type) {
		case action.FileAction:
			if _, ok := v.(*action.AddFile); !ok {
				onlyAddFiles = false
			}
		}
	}
	isBlindAppend := !dependsOnFiles && onlyAddFiles

	commandCtx := make(map[string]string)
	commitInfo := &action.CommitInfo{
		Version:             nil,
		Timestamp:           trx.clock.NowInMillis(),
		UserID:              nil,
		UserName:            nil,
		Operation:           op.Name.String(),
		OperationParameters: op.Parameters,
		Job:                 action.JobInfoFromContext(commandCtx),
		Notebook:            action.NotebookInfoFromContext(commandCtx),
		ClusterId:           util.OptionalToPtr(util.GetMapValueOptional(commandCtx, "clusterId")),
		ReadVersion:         util.OptionalFilterToPtr(mo.Some(trx.readVersion()), func(v int64) bool { return v >= 0 }),
		IsolationLevel:      util.OptionalToPtr(mo.Some(isolationLevelToUse.String())),
		IsBlindAppend:       util.OptionalToPtr(mo.Some(isBlindAppend)),
		OperationMetrics:    nil,
		UserMetadata:        util.OptionalToPtr(op.UserMetadata),
		EngineInfo:          &engineInfo,
	}

	preparedActions = append([]action.Action{commitInfo}, preparedActions...)

	commitVersion, err := trx.doCommitRetryIteratively(trx.snapshot.Version()+1, preparedActions, isolationLevelToUse)
	if err != nil {
		return CommitResult{}, err
	}

	if err := trx.postCommit(commitVersion); err != nil {
		return CommitResult{}, err
	}

	return CommitResult{Version: commitVersion}, nil
}

// Mark files matched by the readPredicate as read by this transaction.
// Please note filtering is only supported on partition columns, thus the files matched may be a superset of the files in the Delta table that satisfy readPredicate.
// Users should use Scan.ResidualPredicate() to check for any unapplied portion of the input predicate.
// Internally, readPredicate and the matched readFiles will be used to determine if logical conflicts between this transaction and previously-committed transactions can be resolved (i.e. no error thrown).
// For example:
//   - This transaction TXN1 reads partition 'date=2021-09-08' to perform an UPDATE and tries to commit at the next table version N.
//   - After TXN1 starts, another transaction TXN2 reads partition 'date=2021-09-07' and commits first at table version N (with no other metadata changes).
//   - TXN1 sees that another commit won, and needs to know whether to commit at version N+1 or fail.
//     Using the readPredicates and resultant readFiles, TXN1 can see that none of its read files were changed by TXN2. Thus there are no logical conflicts and TXN1 can commit at table version N+1.
func (trx *optimisticTransactionImp) MarkFilesAsRead(readPredicate types.Expression) (Scan, error) {
	scan, err := trx.snapshot.Scan(readPredicate)
	if err != nil {
		return nil, err
	}

	matchedFiles, err := scan.Files()
	if err != nil {
		return nil, err
	}

	if scan.PushedPredicate() != nil {
		trx.readPredicates = append(trx.readPredicates, scan.PushedPredicate())
	}

	for f, err := matchedFiles.Next(); err == nil; f, err = matchedFiles.Next() {
		trx.readFiles.Add(f)
	}
	if err != nil && err != io.EOF {
		return nil, err
	}

	matchedFiles.Close()

	return scan, nil
}

// Records an update to the metadata that should be committed with this transaction.
// IMPORTANT: It is the responsibility of the caller to ensure that files currently present in the table are still valid under the new meta
func (trx *optimisticTransactionImp) UpdateMetadata(metadata *action.Metadata) error {
	//latestMetadata := metadata

	// this Metadata instance was previously added
	if trx.newMetadata.IsPresent() && trx.newMetadata.MustGet().Equals(metadata) {
		return nil
	}

	if trx.newMetadata.IsPresent() {
		return errors.New("cannot change the metadata more than once in a transaction")
	}

	if trx.readVersion() == -1 || trx.isCreatingNewTable {
		metadata = trx.withGlobalConfigDefaults(metadata)
		trx.isCreatingNewTable = true
	}

	m, err := trx.snapshot.Metadata()
	if err != nil {
		return err
	}

	latestSchema, err := metadata.Schema()
	if err != nil {
		return err
	}

	if m.SchemaString != metadata.SchemaString {
		if err := types.CheckUnenforceableNotNullConstraints(latestSchema); err != nil {
			return err
		}
	}

	if err := trx.verifyNewMetadata(metadata); err != nil {
		return err
	}
	if err := trx.checkPartitionColumns(metadata.PartitionColumns, latestSchema); err != nil {
		return err
	}

	trx.newMetadata = mo.Some(metadata)

	return nil
}

// the metadata for this transaction.
// It refers to the metadata of the table's latest version as of this transaction's instantiation unless updated during the transaction.
func (trx *optimisticTransactionImp) Metadata() (*action.Metadata, error) {
	if trx.newMetadata.IsPresent() {
		return trx.newMetadata.MustGet(), nil
	}
	t, err := trx.snapshot.protocolAndMetadata.Get()
	if err != nil {
		return nil, err
	}
	return t.V2, nil
}

// Mark the entire table as tainted (i.e. read) by this transaction.
func (trx *optimisticTransactionImp) ReadWholeTable() error {
	trx.readPredicates = append(trx.readPredicates, types.True)
	trx.readTheWholeTable = true
	return nil
}

// returns the latest version that has committed for the idempotent transaction with given id.
func (trx *optimisticTransactionImp) TxnVersion(id string) (int64, error) {
	trx.readTxn = append(trx.readTxn, id)
	if v, ok := trx.snapshot.transactions()[id]; ok {
		return v, nil
	} else {
		return -1, nil
	}
}

func (trx *optimisticTransactionImp) withGlobalConfigDefaults(metadata *action.Metadata) *action.Metadata {

	newMetadata := &action.Metadata{}
	deepcopier.Copy(metadata).To(newMetadata)
	newMetadata.Configuration = mergeGlobalTableConfigurations(trx.configurations, metadata.Configuration)
	return newMetadata
}

func (trx *optimisticTransactionImp) verifyNewMetadata(metadata *action.Metadata) error {
	// the whole schema
	schema, err := metadata.Schema()
	if err != nil {
		return err
	}

	if err := types.CheckColumnNameDuplication(schema, "in the metadata update"); err != nil {
		return err
	}

	// data schema
	dataSchema, err := metadata.DataSchema()
	if err != nil {
		return err
	}

	if err := types.CheckFieldNames(types.ExplodeNestedFieldNames(dataSchema)); err != nil {
		return err
	}

	// partition columns
	if err := types.CheckFieldNames(metadata.PartitionColumns); err != nil {
		return errno.InvalidPartitionColumn(err)
	}

	return action.CheckMetadataProtocolProperties(metadata, nil)
}

func (trx *optimisticTransactionImp) checkPartitionColumns(partitionCols []string, schema *types.StructType) error {
	schemaCols := mapset.NewSet(schema.FieldNames()...)
	partitionColsNotInSchema := mapset.NewSet(partitionCols...).Difference(schemaCols)

	if partitionColsNotInSchema.Cardinality() != 0 {
		return errno.PartitionColumnsNotFoundError(partitionColsNotInSchema.ToSlice(), types.ForceToJSON(schema))
	}

	if len(partitionCols) == schemaCols.Cardinality() {
		return errno.NonPartitionColumnAbsentError()
	}
	return nil
}

func (trx *optimisticTransactionImp) verifySchemaCompatibility(
	existingSchema *types.StructType,
	newSchema *types.StructType,
	actions []action.Action) error {

	numFiles, err := trx.snapshot.numOfFiles()
	if err != nil {
		return err
	}
	if numFiles == 0 {
		return nil
	}

	removeFilesSet := mapset.NewSet[string]()
	for _, a := range actions {
		if r, ok := a.(*action.RemoveFile); ok {
			removeFilesSet.Add(r.Path)
		}
	}

	allFiles, err := trx.snapshot.AllFiles()
	if err != nil {
		return err
	}
	allFilePaths := fp.Map(func(a *action.AddFile) string { return a.Path })(allFiles)
	allFilesSet := mapset.NewSet(allFilePaths...)

	if removeFilesSet.Equal(allFilesSet) {
		return nil
	}

	if !types.IsWriteCompatible(existingSchema, newSchema) {
		return errno.SchemaChangeError(types.ForceToJSON(existingSchema), types.ForceToJSON(newSchema))
	}

	return nil
}

func (trx *optimisticTransactionImp) prepareCommit(actions []action.Action) ([]action.Action, error) {
	if trx.committed {
		return nil, errno.AssertionError("Trasaction already committed")
	}

	finalActions := make([]action.Action, len(actions))
	for i, a := range actions {
		switch v := a.(type) {
		case *action.CommitInfo:
			return nil, errno.AssertionError("Cannot commit a ccustom CommitInfo in a transaction")
		case *action.AddFile:
			rel, err := path.Relative(trx.logPath, v.Path)
			if err != nil {
				return nil, err
			}
			finalActions[i] = v.Copy(v.DataChange, rel)
		default:
			finalActions[i] = a
		}
	}

	if trx.newMetadata.IsPresent() {
		newSchema, err := trx.newMetadata.MustGet().Schema()
		if err != nil {
			return nil, err
		}
		existingMeta, err := trx.snapshot.Metadata()
		if err != nil {
			return nil, err
		}
		existingSchema, err := existingMeta.Schema()
		if err != nil {
			return nil, err
		}

		if err := trx.verifySchemaCompatibility(existingSchema, newSchema, actions); err != nil {
			return nil, err
		}

		finalActions = append([]action.Action{trx.newMetadata.MustGet()}, finalActions...)
	}

	if trx.snapshot.version == -1 {
		exist, err := trx.logStore.Exists(trx.logPath)
		if err != nil {
			return nil, err
		}
		if !exist {
			if err := trx.logStore.Create(trx.logPath); err != nil {
				return nil, err
			}
		}

		if !util.Exists(finalActions, func(a action.Action) bool {
			_, ok := a.(*action.Protocol)
			return ok
		}) {
			protocol, err := trx.protocol()
			if err != nil {
				return nil, err
			}
			finalActions = append([]action.Action{protocol}, finalActions...)
		}

		if !util.Exists(finalActions, func(a action.Action) bool {
			_, ok := a.(*action.Metadata)
			return ok
		}) {
			return nil, errno.MetadataAbsentError()
		}
	}

	protocolOpt := mo.None[*action.Protocol]()
	for _, a := range finalActions {
		if p, ok := a.(*action.Protocol); ok {
			protocolOpt = mo.Some(p)
		}
	}
	if protocolOpt.IsPresent() && !protocolOpt.MustGet().Equals(action.DefaultProtocol()) {
		return nil, errno.AssertionError("Currently only Protocol readerVersion 1 and writerVersion 2 is supported.")
	}

	metadata, err := trx.Metadata()
	if err != nil {
		return nil, err
	}

	partitionColumns := mapset.NewSet(metadata.PartitionColumns...)
	for _, a := range finalActions {
		switch v := a.(type) {
		case *action.AddFile:

			if !util.KeySet(v.PartitionValues).Equal(partitionColumns) {
				return nil, errno.AddFilePartitioningMismatchError()
			}
		}
	}

	var removes []*action.RemoveFile
	for _, a := range actions {
		if v, ok := a.(*action.RemoveFile); ok {
			removes = append(removes, v)
		}
	}
	if util.Exists(removes, func(e *action.RemoveFile) bool { return e.DataChange }) {
		if DeltaConfigIsAppendOnly.fromMetadata(metadata) {
			return nil, errno.ModifyAppendOnlyTableError()
		}
	}

	return finalActions, nil
}

func (trx *optimisticTransactionImp) doCommitRetryIteratively(attemptVersion int64, actions []action.Action, isolationLevel isolation.Level) (int64, error) {
	trx.lock.Lock()
	defer trx.lock.Unlock()

	tryCommit := true
	commitVersion := attemptVersion
	attemptNumber := 0

	for tryCommit {
		var errCommit error
		if attemptNumber == 0 {
			_, errCommit = trx.doCommit(commitVersion, actions, isolationLevel)
		} else if attemptVersion > DELTA_MAX_RETRY_COMMIT_ATTEMPTS {
			return 0, errno.MaxCommitRetriesExceededError("max commit attempts exceeded")
		} else {
			commitVersion, err := trx.checkForConflicts(commitVersion, actions, attemptNumber, isolationLevel)
			if err != nil {
				return 0, err
			}
			_, errCommit = trx.doCommit(commitVersion, actions, isolationLevel)
		}

		if errCommit == nil {
			tryCommit = false
			break
		}

		if eris.Is(errCommit, errno.ErrFileAlreadyExists) {
			attemptNumber++
		} else {
			return 0, errCommit
		}
	}

	return commitVersion, nil
}

func (trx *optimisticTransactionImp) doCommit(attemptVersion int64, actions []action.Action, isolationLevel isolation.Level) (int64, error) {
	// we ignoore somoe logs here

	actionStrings, err := action.UtilFnMapToString(actions)
	if err != nil {
		return 0, err
	}

	if err := trx.logStore.Write(filenames.DeltaFile(trx.logStore.Root(), attemptVersion),
		iter.FromSlice(actionStrings), false); err != nil {
		return 0, err
	}

	// note: we use updateInternal so we don't need to use the reentrant lock, but if some bugs happened, check this.
	s, err := trx.snapshotManager.updateInternal()
	if err != nil {
		return 0, err
	}
	if s.Version() < attemptVersion {
		return 0, errno.IllegalStateError(fmt.Sprintf("the commit version is %d, but the current version is %d", attemptVersion, s.Version()))
	}

	return attemptVersion, nil
}

func (trx *optimisticTransactionImp) checkForConflicts(checkVersion int64, actions []action.Action, attemptNumber int, commitIsolationLevel isolation.Level) (int64, error) {
	nextAttemptVersion, err := trx.getNextAttemptVersion()
	if err != nil {
		return 0, err
	}

	metadata, err := trx.Metadata()
	if err != nil {
		return 0, err
	}

	currentTransactionInfo := &currentTransactionInfo{
		readPredicates: trx.readPredicates,
		readFiles:      trx.readFiles,
		readWholeTable: trx.readTheWholeTable,
		readAppIds:     mapset.NewSet(trx.readTxn...),
		metadata:       metadata,
		actions:        actions,
		logStore:       trx.logStore,
		logPath:        trx.logStore.Root(),
	}

	for otherCommitVersion := checkVersion; otherCommitVersion < nextAttemptVersion; otherCommitVersion++ {
		conflictChecker, err := newConflictChecker(currentTransactionInfo, otherCommitVersion, commitIsolationLevel)
		if err != nil {
			return 0, err
		}
		if err := conflictChecker.checkConflicts(); err != nil {
			return 0, err
		}
	}

	return nextAttemptVersion, nil
}

func (trx *optimisticTransactionImp) getNextAttemptVersion() (int64, error) {
	// here we call updateInternal without locking
	if s, err := trx.snapshotManager.updateInternal(); err != nil {
		return 0, err
	} else {
		return 1 + s.Version(), nil
	}
}

func (trx *optimisticTransactionImp) postCommit(commitVersion int64) error {
	trx.committed = true

	metadata, err := trx.snapshot.Metadata()
	if err != nil {
		return err
	}
	checkpointInterval := DeltaConfigCheckpointInterval.fromMetadata(metadata)

	if trx.shouldCheckpoint(commitVersion, checkpointInterval) {
		snaptshot, err := trx.snapshotManager.getSnapshotForVersionAsOf(commitVersion)
		if err != nil {
			return err
		}
		if err := checkpoint(trx.logPath, trx.logStore, snaptshot); err != nil {
			if eris.Is(err, errno.ErrIllegalState) {
				log.Println("Failed to checkpoint table state." + err.Error())
			} else {
				return err
			}
		}
	}

	return nil
}

func (trx *optimisticTransactionImp) shouldCheckpoint(committedVersion int64, checkpointInterval int) bool {
	return committedVersion != 0 && committedVersion%int64(checkpointInterval) == 0
}
