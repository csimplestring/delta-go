package deltago

import (
	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/csimplestring/delta-go/isolation"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/store"
	"github.com/csimplestring/delta-go/types"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/repeale/fp-go"
	"github.com/samber/mo"
)

type currentTransactionInfo struct {
	readPredicates []types.Expression
	readFiles      mapset.Set[*action.AddFile]
	readWholeTable bool
	readAppIds     mapset.Set[string]
	metadata       *action.Metadata
	actions        []action.Action
	logStore       store.Store
	logPath        string
}

type winningCommitSummary struct {
	metadataUpdates       []*action.Metadata
	appLevelTransactions  []*action.SetTransaction
	protocol              []*action.Protocol
	commitInfo            mo.Option[*action.CommitInfo]
	removedFiles          []*action.RemoveFile
	addedFiles            []*action.AddFile
	isBlindAppendOption   mo.Option[bool]
	blindAppendAddedFiles []*action.AddFile
	changedDataAddedFiles []*action.AddFile
	onlyAddFiles          bool
}

func newWinningCommitSummary(actions []action.Action, commitVersion int64) *winningCommitSummary {
	w := &winningCommitSummary{}
	w.metadataUpdates = action.UtilFnCollect[*action.Metadata](actions)
	w.appLevelTransactions = action.UtilFnCollect[*action.SetTransaction](actions)
	w.protocol = action.UtilFnCollect[*action.Protocol](actions)
	w.commitInfo = action.UtilFnCollectFirst[*action.CommitInfo](actions).Map(func(value *action.CommitInfo) (*action.CommitInfo, bool) {
		return value.Copy(commitVersion), true
	})
	w.removedFiles = action.UtilFnCollect[*action.RemoveFile](actions)
	w.addedFiles = action.UtilFnCollect[*action.AddFile](actions)
	w.isBlindAppendOption = util.MapOptional(w.commitInfo, func(v *action.CommitInfo) bool {
		return *v.IsBlindAppend
	})

	if w.isBlindAppendOption.OrElse(false) {
		w.blindAppendAddedFiles = w.addedFiles
	}
	if !w.isBlindAppendOption.OrElse(false) {
		w.changedDataAddedFiles = w.addedFiles
	}

	w.onlyAddFiles = fp.Every(func(t action.FileAction) bool {
		_, ok := t.(*action.AddFile)
		return ok
	})(action.UtilFnCollect[action.FileAction](actions))

	return w
}

type conflictChecker struct {
	currentTransactionInfo *currentTransactionInfo
	winningCommitVersion   int64
	isolationLevel         isolation.Level
	winningCommitSummary   *winningCommitSummary
}

func newConflictChecker(currentTransactionInfo *currentTransactionInfo, winningCommitVersion int64, isolationLevel isolation.Level) (*conflictChecker, error) {
	c := &conflictChecker{
		currentTransactionInfo: currentTransactionInfo,
		winningCommitVersion:   winningCommitVersion,
		isolationLevel:         isolationLevel,
	}
	w, err := c.createWinningCommitSummary()
	if err != nil {
		return nil, err
	}
	c.winningCommitSummary = w
	return c, nil
}

// must call it when crreating conflictChecker
func (c *conflictChecker) createWinningCommitSummary() (*winningCommitSummary, error) {
	trx := c.currentTransactionInfo
	siter, err := trx.logStore.Read(filenames.DeltaFile(trx.logPath, c.winningCommitVersion))
	if err != nil {
		return nil, err
	}
	defer siter.Close()

	winningCommitActions, err := iter.Map(siter, action.FromJson)
	if err != nil {
		return nil, err
	}

	return newWinningCommitSummary(winningCommitActions, c.winningCommitVersion), nil
}

func (c *conflictChecker) checkConflicts() error {
	checks := []func() error{
		c.checkProtocolCompatibility,
		c.checkNoMetadataUpdates,
		c.checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn,
		c.checkForDeletedFilesAgainstCurrentTxnReadFiles,
		c.checkForDeletedFilesAgainstCurrentTxnDeletedFiles,
		c.checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn,
	}
	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (c *conflictChecker) checkProtocolCompatibility() error {
	if len(c.winningCommitSummary.protocol) == 0 {
		return nil
	}

	for _, p := range c.winningCommitSummary.protocol {
		if err := assertProtocolRead(p); err != nil {
			return err
		}
		if err := assertProtocolWrite(p); err != nil {
			return err
		}
	}
	for _, a := range c.currentTransactionInfo.actions {
		if _, ok := a.(*action.Protocol); ok {
			return errno.ProtocolChangedException("")
		}
	}
	return nil
}

func (c *conflictChecker) checkNoMetadataUpdates() error {
	if len(c.winningCommitSummary.metadataUpdates) != 0 {
		return errno.MetadataChangedError()
	}
	return nil
}

func filterFileList(paritionSchema *types.StructType, files []*action.AddFile, filter types.Expression) ([]*action.AddFile, error) {

	var res []*action.AddFile
	for _, file := range files {
		r := &PartitionRowRecord{partitionSchema: paritionSchema, partitionValues: file.PartitionValues}
		v, err := filter.Eval(r)
		if err != nil {
			return nil, err
		}
		if v.(bool) {
			res = append(res, file)
		}
	}
	return res, nil
}

func (c *conflictChecker) checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn() error {
	addedFilesToCheckForConflicts := []*action.AddFile{}
	if c.isolationLevel == isolation.Serializable {
		addedFilesToCheckForConflicts = append(c.winningCommitSummary.changedDataAddedFiles, c.winningCommitSummary.blindAppendAddedFiles...)
	}

	mSchema, err := c.currentTransactionInfo.metadata.PartitionSchema()
	if err != nil {
		return err
	}

	for _, p := range c.currentTransactionInfo.readPredicates {
		files, err := filterFileList(mSchema, addedFilesToCheckForConflicts, p)
		if err != nil {
			return err
		}
		if len(files) != 0 {
			return errno.ConcurrentAppend("")
		}
	}

	return nil
}

func (c *conflictChecker) checkForDeletedFilesAgainstCurrentTxnReadFiles() error {
	readFilePaths := make(map[string]map[string]string)
	for f := range c.currentTransactionInfo.readFiles.Iterator().C {
		readFilePaths[f.Path] = f.PartitionValues
	}

	var deleteReadOverlap *action.RemoveFile
	for _, f := range c.winningCommitSummary.removedFiles {
		if _, ok := readFilePaths[f.Path]; ok {
			deleteReadOverlap = f
		}
	}

	if deleteReadOverlap != nil {
		return errno.ConcurrentDeleteRead(deleteReadOverlap.Path)
	}
	if len(c.winningCommitSummary.removedFiles) != 0 && c.currentTransactionInfo.readWholeTable {
		return errno.ConcurrentDeleteRead(c.winningCommitSummary.removedFiles[0].Path)
	}
	return nil
}

func (c *conflictChecker) checkForDeletedFilesAgainstCurrentTxnDeletedFiles() error {
	deletedPaths := fp.Map(func(t *action.RemoveFile) string {
		return t.Path
	})(action.UtilFnCollect[*action.RemoveFile](c.currentTransactionInfo.actions))
	txnDeletes := mapset.NewSet(deletedPaths...)

	winningRemoved := fp.Map(func(t *action.RemoveFile) string {
		return t.Path
	})(c.winningCommitSummary.removedFiles)
	winningRemovedPaths := mapset.NewSet(winningRemoved...)

	deleteOverlap := winningRemovedPaths.Intersect(txnDeletes)
	if deleteOverlap.Cardinality() != 0 {
		file, _ := deleteOverlap.Pop()
		return errno.ConcurrentDeleteDelete(file)
	}
	return nil
}

func (c *conflictChecker) checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn() error {
	appIds := fp.Map(func(t *action.SetTransaction) string {
		return t.AppId
	})(c.winningCommitSummary.appLevelTransactions)

	if mapset.NewSet(appIds...).Intersect(c.currentTransactionInfo.readAppIds).Cardinality() != 0 {
		return &errno.ConcurrentTransactionError{}
	}
	return nil
}

func assertProtocolRead(protocol *action.Protocol) error {
	if protocol != nil && action.ReaderVersion < protocol.MinReaderVersion {
		return errno.InvalidProtocolVersionError()
	}
	return nil
}

func assertProtocolWrite(protocol *action.Protocol) error {
	if protocol != nil && action.WriterVersion < protocol.MinWriterVersion {
		return errno.InvalidProtocolVersionError()
	}
	return nil
}
