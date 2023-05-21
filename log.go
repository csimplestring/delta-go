package deltago

import (
	"io"
	"strings"
	"sync"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/store"
	"github.com/rotisserie/eris"
)

// Log Represents the transaction logs of a Delta table.
// It provides APIs to access the states of a Delta table.
type Log interface {

	// Snapshot the current Snapshot of the Delta table.
	// You may need to call update() to access the latest snapshot if the current snapshot is stale.
	Snapshot() (Snapshot, error)

	Update() (Snapshot, error)

	SnapshotForVersionAsOf(version int64) (Snapshot, error)

	SnapshotForTimestampAsOf(timestamp int64) (Snapshot, error)

	StartTransaction() (OptimisticTransaction, error)

	CommitInfoAt(version int64) (*action.CommitInfo, error)

	Path() string

	// Get all actions starting from startVersion (inclusive) in increasing order of committed version.
	// If startVersion doesn't exist, return an empty Iterator.
	Changes(startVersion int64, failOnDataLoss bool) (iter.Iter[VersionLog], error)

	// Returns the latest version that was committed before or at timestamp. If no version exists, returns -1. Specifically:
	// if a commit version exactly matches the provided timestamp, we return it
	// else, we return the latest commit version with a timestamp less than the provided one
	// If the provided timestamp is less than the timestamp of any committed version, we throw an error.
	VersionBeforeOrAtTimestamp(timestamp int64) (int64, error)

	// Returns the latest version that was committed at or after timestamp. If no version exists, returns -1. Specifically:
	// if a commit version exactly matches the provided timestamp, we return it
	// else, we return the earliest commit version with a timestamp greater than the provided one
	// If the provided timestamp is larger than the timestamp of any committed version, we throw an error.
	VersionAtOrAfterTimestamp(timestamp int64) (int64, error)

	TableExists() bool
}

// ForTable Create a DeltaLog instance representing the table located at the provided path.
func ForTable(dataPath string, config Config, clock Clock) (Log, error) {
	logPath := strings.TrimRight(dataPath, "/") + "/_delta_log/"

	deltaLogLock := &sync.Mutex{}
	var logStore store.Store

	logStore, err := store.New(logPath)
	if err != nil {
		return nil, err
	}

	parquetReader, err := newCheckpointReader(logPath)
	if err != nil {
		return nil, err
	}

	historyManager := &historyManager{logStore: logStore}
	snaptshotManager, err := newSnapshotReader(config, parquetReader, logStore, clock, historyManager, deltaLogLock)
	if err != nil {
		return nil, err
	}

	logImpl := &logImpl{
		dataPath:       dataPath,
		logPath:        logPath,
		clock:          clock,
		store:          logStore,
		deltaLogLock:   deltaLogLock,
		history:        historyManager,
		snapshotReader: snaptshotManager,
	}

	return logImpl, nil
}

// func ForTableV2(config Config, clock Clock) (Log, error) {
// 	deltaLogLock := &sync.Mutex{}
// 	var logStore store.Store
// 	var fs store.FS

// 	logStore, err := configureLogStore(config)
// 	if err != nil {
// 		return nil, err
// 	}

// 	fs, err = store.GetFileSystem(logPath)
// 	if err != nil {
// 		return nil, err
// 	}

// 	parquetReader, err := newCheckpointReader(config)
// 	if err != nil {
// 		return nil, err
// 	}

// 	historyManager := &historyManager{logStore: logStore}
// 	snaptshotManager, err := newSnapshotReader(config, parquetReader, logStore, clock, historyManager, deltaLogLock)
// 	if err != nil {
// 		return nil, err
// 	}

// 	logImpl := &logImpl{
// 		dataPath:       dataPath,
// 		logPath:        logPath,
// 		clock:          clock,
// 		store:          logStore,
// 		fs:             fs,
// 		deltaLogLock:   deltaLogLock,
// 		history:        historyManager,
// 		snapshotReader: snaptshotManager,
// 	}

// 	return logImpl, nil
// }

type logImpl struct {
	dataPath       string
	logPath        string
	clock          Clock
	store          store.Store
	deltaLogLock   *sync.Mutex
	history        *historyManager
	snapshotReader *SnapshotReader
}

// Snapshot the current Snapshot of the Delta table.
// You may need to call update() to access the latest snapshot if the current snapshot is stale.
func (l *logImpl) Snapshot() (Snapshot, error) {
	return l.snapshotReader.currentSnapshot.Load(), nil
}

func (l *logImpl) Update() (Snapshot, error) {
	return l.snapshotReader.update()
}

func (l *logImpl) SnapshotForVersionAsOf(version int64) (Snapshot, error) {
	return l.snapshotReader.getSnapshotForVersionAsOf(version)
}

func (l *logImpl) SnapshotForTimestampAsOf(timestamp int64) (Snapshot, error) {
	return l.snapshotReader.getSnapshotForTimestampAsOf(timestamp)
}

func (l *logImpl) StartTransaction() (OptimisticTransaction, error) {
	snapshot, err := l.snapshotReader.update()
	if err != nil {
		return nil, err
	}
	return newOptimisticTransaction(snapshot,
		l.snapshotReader, l.clock, nil, l.deltaLogLock, l.store, l.logPath), nil
}

func (l *logImpl) CommitInfoAt(version int64) (*action.CommitInfo, error) {

	if err := l.history.checkVersionExists(version, l.snapshotReader); err != nil {
		return nil, err
	}

	return l.history.getCommitInfo(version)
}

func (l *logImpl) Path() string {
	return l.dataPath
}

// Get all actions starting from startVersion (inclusive) in increasing order of committed version.
// If startVersion doesn't exist, return an empty Iterator.
func (l *logImpl) Changes(startVersion int64, failOnDataLoss bool) (iter.Iter[VersionLog], error) {
	if startVersion < 0 {
		return nil, eris.Wrap(errno.ErrIllegalArgument, "invalid startVersion")
	}

	fs, err := l.store.ListFrom(filenames.DeltaFile(l.logPath, startVersion))
	if err != nil {
		return nil, err
	}
	defer fs.Close()

	var deltaPaths []string
	for f, err := fs.Next(); err == nil; f, err = fs.Next() {
		if filenames.IsDeltaFile(f.Path()) {
			deltaPaths = append(deltaPaths, f.Path())
		}
	}
	if err != nil && err != io.EOF {
		return nil, err
	}

	lastSeenVersion := startVersion - 1
	versionLogs := make([]VersionLog, len(deltaPaths))
	for i, deltaPath := range deltaPaths {
		version := filenames.DeltaVersion(deltaPath)
		if failOnDataLoss && version > lastSeenVersion+1 {
			return nil, errno.IllegalStateError("fail on data loss")
		}
		lastSeenVersion = version

		versionLogs[i] = &MemOptimizedVersionLog{
			version: version,
			path:    deltaPath,
			store:   l.store,
		}
	}

	return iter.FromSlice(versionLogs), nil
}

// Returns the latest version that was committed before or at timestamp. If no version exists, returns -1. Specifically:
// if a commit version exactly matches the provided timestamp, we return it
// else, we return the latest commit version with a timestamp less than the provided one
// If the provided timestamp is less than the timestamp of any committed version, we throw an error.
func (l *logImpl) VersionBeforeOrAtTimestamp(timestamp int64) (int64, error) {
	if !l.TableExists() {
		return -1, nil
	}

	canReturnLastCommit := true
	mustBeRecreatable := false
	canReturnEarliestCommit := false

	c, err := l.history.getActiveCommitAtTime(l.snapshotReader,
		timestamp,
		canReturnLastCommit,
		mustBeRecreatable,
		canReturnEarliestCommit)
	if err != nil {
		return -1, err
	}

	return c.version, nil
}

// Returns the latest version that was committed at or after timestamp. If no version exists, returns -1. Specifically:
// if a commit version exactly matches the provided timestamp, we return it
// else, we return the earliest commit version with a timestamp greater than the provided one
// If the provided timestamp is larger than the timestamp of any committed version, we throw an error.
func (l *logImpl) VersionAtOrAfterTimestamp(timestamp int64) (int64, error) {

	if !l.TableExists() {
		return -1, nil
	}

	canReturnLastCommit := false
	mustBeRecreatable := false
	canReturnEarliestCommit := true

	c, err := l.history.getActiveCommitAtTime(l.snapshotReader,
		timestamp,
		canReturnLastCommit,
		mustBeRecreatable,
		canReturnEarliestCommit)
	if err != nil {
		return -1, err
	}

	if c.timestamp >= timestamp {
		return c.version, nil
	} else {
		return c.version + 1, nil
	}
}

func (l *logImpl) TableExists() bool {
	return l.snapshotReader.snapshot().Version() >= 0
}
