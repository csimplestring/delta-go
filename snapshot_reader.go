package deltago

import (
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ahmetb/go-linq/v3"
	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/csimplestring/delta-go/store"
	"github.com/rotisserie/eris"
	"github.com/samber/mo"

	mapset "github.com/deckarep/golang-set/v2"
)

type SnapshotReader struct {
	logStore         store.Store
	config           Config
	checkpointReader checkpointReader
	clock            Clock
	history          *historyManager

	// internal use only
	mu              *sync.Mutex
	currentSnapshot atomic.Pointer[snapshotImp]
}

func newSnapshotReader(config Config, cpReader checkpointReader, logStore store.Store, clock Clock, history *historyManager, mu *sync.Mutex) (*SnapshotReader, error) {
	s := &SnapshotReader{
		logStore:         logStore,
		config:           config,
		checkpointReader: cpReader,
		clock:            clock,
		history:          history,
		mu:               mu,
	}

	initSnapshot, err := s.getSnapshotAtInit()
	if err != nil {
		return nil, err
	}

	// load it as an atomic reference
	s.currentSnapshot.Store(initSnapshot)

	return s, nil
}

func (sr *SnapshotReader) getMinFileRetentionTimestamp() (int64, error) {
	var metadata *action.Metadata
	var err error

	if sr.snapshot() == nil {
		metadata = &action.Metadata{}
	} else {
		metadata, err = sr.snapshot().Metadata()
	}

	if err != nil {
		return 0, err
	}
	// in milliseconds
	tombstoneRetention := DeltaConfigTombstoneRetention.fromMetadata(metadata)
	return sr.clock.NowInMillis() - tombstoneRetention.Milliseconds(), nil
}

func (sr *SnapshotReader) snapshot() *snapshotImp {
	return sr.currentSnapshot.Load()
}

func (sr *SnapshotReader) getSnapshotAtInit() (*snapshotImp, error) {
	lastCheckpoint, err := LastCheckpoint(sr.logStore)
	if err != nil {
		return nil, eris.Wrap(err, "last checkpoint")
	}

	logSegment, err := sr.getLogSegmentForVersion(
		util.MapOptional(lastCheckpoint, func(v *CheckpointMetaDataJSON) int64 { return v.Version }),
		mo.None[int64](),
	)
	if err != nil {
		if eris.Is(err, errno.ErrFileNotFound) {
			return newInitialSnapshotImp(sr.config, sr.logStore.Root(), sr.logStore, sr.checkpointReader)
		}
		return nil, err
	}

	return sr.createSnapshot(logSegment, logSegment.LastCommitTimestamp.UnixMilli())
}

func (sr *SnapshotReader) getSnapshotAt(version int64) (*snapshotImp, error) {
	if sr.snapshot().Version() == version {
		return sr.snapshot(), nil
	}

	startingCheckpoint, err := FindLastCompleteCheckpoint(sr.logStore, CheckpointInstance{Version: version, NumParts: mo.None[int]()})
	if err != nil {
		return nil, err
	}

	start := util.MapOptional(startingCheckpoint, func(v *CheckpointInstance) int64 { return v.Version })
	segment, err := sr.getLogSegmentForVersion(start, mo.Some(version))
	if err != nil {
		return nil, err
	}

	return sr.createSnapshot(segment, segment.LastCommitTimestamp.UnixMilli())
}

func (sr *SnapshotReader) getSnapshotForVersionAsOf(version int64) (*snapshotImp, error) {

	if err := sr.history.checkVersionExists(version, sr); err != nil {
		return nil, err
	}
	return sr.getSnapshotAt(version)
}

func (sr *SnapshotReader) getSnapshotForTimestampAsOf(timestamp int64) (*snapshotImp, error) {

	latestCommit, err := sr.history.getActiveCommitAtTime(sr, timestamp, false, true, false)
	if err != nil {
		return nil, err
	}
	return sr.getSnapshotAt(latestCommit.version)
}

func (sr *SnapshotReader) getLogSegmentForVersion(startCheckpoint mo.Option[int64], versionToLoad mo.Option[int64]) (*LogSegment, error) {

	iter, err := sr.logStore.ListFrom(filenames.CheckpointPrefix(sr.logStore.Root(), startCheckpoint.OrElse(0)))
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var newFiles []*store.FileMeta
	// List from the starting  If a checkpoint doesn't exist, this will still return
	// deltaVersion=0.
	for f, err := iter.Next(); err == nil; f, err = iter.Next() {
		if !(filenames.IsCheckpointFile(f.Path()) || filenames.IsDeltaFile(f.Path())) {
			continue
		}
		if filenames.IsCheckpointFile(f.Path()) && f.Size() == 0 {
			continue
		}
		v, err := filenames.GetFileVersion(f.Path())
		if err != nil {
			return nil, err
		}
		if versionToLoad.IsAbsent() || (versionToLoad.IsPresent() && v <= versionToLoad.MustGet()) {
			newFiles = append(newFiles, f)
		} else {
			break
		}
	}
	if err != nil && err != io.EOF {
		return nil, err
	}

	if len(newFiles) == 0 && startCheckpoint.IsAbsent() {
		return nil, eris.Wrap(errno.EmptyDirectoryError(sr.logStore.Root()), "")
	} else if len(newFiles) == 0 {
		// The directory may be deleted and recreated and we may have stale state in our DeltaLog
		// singleton, so try listing from the first version
		return sr.getLogSegmentForVersion(mo.None[int64](), versionToLoad)
	}

	checkpoints, deltas := splitDeltaAndCheckpoint(newFiles)

	var lastCheckpoint CheckpointInstance
	if versionToLoad.IsAbsent() {
		lastCheckpoint = MaxInstance
	} else {
		lastCheckpoint = CheckpointInstance{Version: versionToLoad.MustGet()}
	}

	var checkpointFiles []*CheckpointInstance
	linq.From(checkpoints).SelectT(func(f *store.FileMeta) *CheckpointInstance {
		return FromPath(f.Path())
	}).ToSlice(&checkpointFiles)

	newCheckpoint := GetLatestCompleteCheckpointFromList(checkpointFiles, lastCheckpoint)
	if newCheckpoint.IsPresent() {
		newCheckpointVersion := newCheckpoint.MustGet().Version
		newCheckpoinPaths := mapset.NewSet(newCheckpoint.MustGet().GetCorrespondingFiles(sr.logStore.Root())...)

		var deltasAfterCheckpoint []*store.FileMeta
		linq.From(deltas).WhereT(func(f *store.FileMeta) bool {
			return filenames.DeltaVersion(f.Path()) > newCheckpointVersion
		}).ToSlice(&deltasAfterCheckpoint)

		var deltaVersions []int64
		linq.From(deltasAfterCheckpoint).SelectT(func(f *store.FileMeta) int64 {
			return filenames.DeltaVersion(f.Path())
		}).ToSlice(&deltaVersions)

		if len(deltaVersions) != 0 {
			// if err := verifyDeltaVersions(deltaVersions); err != nil {
			// 	return nil, err
			// }
			if deltaVersions[0] != newCheckpointVersion+1 {
				return nil, errno.NoFirstDeltaFile()
			}
			if versionToLoad.IsPresent() && versionToLoad.MustGet() == deltaVersions[len(deltaVersions)-1] {
				return nil, errno.NoLastDeltaFile()
			}
		}

		var newVersion int64
		if len(deltaVersions) != 0 {
			newVersion = deltaVersions[len(deltaVersions)-1]
		} else {
			newVersion = newCheckpoint.MustGet().Version
		}

		var newCheckpointFiles []*store.FileMeta
		linq.From(checkpoints).WhereT(func(f *store.FileMeta) bool {
			return newCheckpoinPaths.Contains(f.Path())
		}).ToSlice(&newCheckpointFiles)

		if len(newCheckpointFiles) != newCheckpoinPaths.Cardinality() {
			return nil, eris.New("failed in getting the file information")
		}

		// In the case where `deltasAfterCheckpoint` is empty, `deltas` should still not be empty,
		// they may just be before the checkpoint version unless we have a bug in log cleanup
		lastCommitTimestamp := deltas[len(deltas)-1].TimeModified()

		return &LogSegment{
			LogPath:             sr.logStore.Root(),
			Version:             newVersion,
			Deltas:              deltasAfterCheckpoint,
			Checkpoints:         newCheckpointFiles,
			CheckpointVersion:   mo.Some(newCheckpointVersion),
			LastCommitTimestamp: lastCommitTimestamp,
		}, nil
	} else {
		// No starting checkpoint found. This means that we should definitely have version 0, or the
		// last checkpoint we thought should exist (the `_last_checkpoint` file) no longer exists
		if startCheckpoint.IsPresent() {
			return nil, errno.MissingPartFile(startCheckpoint.MustGet())
		}

		var deltaVersions []int64
		linq.From(deltas).SelectT(func(f *store.FileMeta) int64 {
			return filenames.DeltaVersion(f.Path())
		}).ToSlice(&deltaVersions)

		if err := verifyDeltaVersions(deltaVersions); err != nil {
			return nil, err
		}

		latestCommit := deltas[len(deltas)-1]
		return &LogSegment{
			LogPath:             sr.logStore.Root(),
			Version:             filenames.DeltaVersion(latestCommit.Path()),
			Deltas:              deltas,
			Checkpoints:         nil,
			CheckpointVersion:   mo.None[int64](),
			LastCommitTimestamp: latestCommit.TimeModified(),
		}, nil
	}
}

func (sr *SnapshotReader) createSnapshot(segment *LogSegment, lastCommitTs int64) (*snapshotImp, error) {
	minFileRetention, err := sr.getMinFileRetentionTimestamp()
	if err != nil {
		return nil, err
	}

	return newSnapshotImp(sr.config, sr.logStore.Root(), segment.Version, segment, minFileRetention, lastCommitTs, sr.logStore, sr.checkpointReader)
}

func (sr *SnapshotReader) update() (*snapshotImp, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	return sr.updateInternal()
}

// updateInternal is not goroutine-safe, the caller should take care of locking.
func (sr *SnapshotReader) updateInternal() (*snapshotImp, error) {
	currentSnapshot := sr.currentSnapshot.Load()
	v := currentSnapshot.logSegment.CheckpointVersion
	segment, err := sr.getLogSegmentForVersion(v, mo.None[int64]())

	if err != nil && eris.Is(err, errno.ErrFileNotFound) {
		if strings.Contains(err.Error(), "reconstruct state at version") {
			return nil, err
		}

		log.Println("No delta log found for the Delta table at " + sr.logStore.Root())
		newSnapshot, err := newInitialSnapshotImp(sr.config, sr.logStore.Root(), sr.logStore, sr.checkpointReader)
		if err != nil {
			return nil, err
		}
		sr.currentSnapshot.Store(newSnapshot)
		return newSnapshot, nil
	}

	if !currentSnapshot.logSegment.equal(segment) {
		newSnapshot, err := sr.createSnapshot(segment, segment.LastCommitTimestamp.UnixMilli())
		if err != nil {
			return nil, err
		}

		sr.currentSnapshot.Store(newSnapshot)
		return newSnapshot, nil
	}

	return currentSnapshot, nil
}

func verifyDeltaVersions(deltaVersions []int64) error {
	if len(deltaVersions) == 0 {
		return nil
	}
	for i := deltaVersions[0]; i <= deltaVersions[len(deltaVersions)-1]; i++ {
		if i != deltaVersions[i] {
			return eris.Wrap(errno.DeltaVersionNotContinuous(deltaVersions), "")
		}
	}
	return nil
}

func splitDeltaAndCheckpoint(a []*store.FileMeta) (checkpoints []*store.FileMeta, deltas []*store.FileMeta) {

	for _, f := range a {
		if filenames.IsCheckpointFile(f.Path()) {
			checkpoints = append(checkpoints, f)
		} else {
			deltas = append(deltas, f)
		}
	}
	return
}
