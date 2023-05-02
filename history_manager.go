package deltago

import (
	"io"
	"math"

	"github.com/barweiss/go-tuple"
	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/csimplestring/delta-go/store"
	"github.com/samber/mo"
)

type historyManager struct {
	logStore store.Store
}

func (h *historyManager) getCommitInfo(version int64) (*action.CommitInfo, error) {
	iter, err := h.logStore.Read(filenames.DeltaFile(h.logStore.Root(), version))
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var c *action.CommitInfo
	for v, err := iter.Next(); err == nil; v, err = iter.Next() {
		action, err := action.FromJson(v)
		if err != nil {
			return nil, err
		}

		if singleAction := action.Wrap(); singleAction != nil && singleAction.CommitInfo != nil {
			c = singleAction.CommitInfo
			break
		}
	}
	if err != nil && err != io.EOF {
		return nil, err
	}

	if c == nil {
		return &action.CommitInfo{Version: &version}, nil
	} else {
		return c.Copy(version), nil
	}
}

func (h *historyManager) checkVersionExists(versionToCkeck int64, sr *SnapshotReader) error {
	earliestVersion, err := h.getEarliestReproducibleCommitVersion()
	if err != nil {
		return err
	}

	s, err := sr.update()
	if err != nil {
		return err
	}
	latestVersion := s.Version()
	if versionToCkeck < earliestVersion || versionToCkeck > latestVersion {
		return errno.VersionNotExist(versionToCkeck, earliestVersion, latestVersion)
	}

	return nil
}

func (h *historyManager) getActiveCommitAtTime(sr *SnapshotReader, timestamp int64,
	canReturnLastCommit bool, mustBeRecreatable bool, canReturnEarliestCommit bool) (*commit, error) {

	timeInMill := timestamp
	var earliestVersion int64
	var err error
	if mustBeRecreatable {
		earliestVersion, err = h.getEarliestReproducibleCommitVersion()
	} else {
		earliestVersion, err = h.getEarliestDeltaFile()
	}
	if err != nil {
		return nil, err
	}

	s, err := sr.update()
	if err != nil {
		return nil, err
	}
	latestVersion := s.Version()

	commits, err := h.getCommits(h.logStore, h.logStore.Root(), earliestVersion, latestVersion+1)
	if err != nil {
		return nil, err
	}

	commit := h.getLastCommitBeforeTimestamp(commits, timeInMill).OrElse(commits[0])

	commitTs := commit.timestamp
	if commit.timestamp > timeInMill && !canReturnEarliestCommit {
		return nil, errno.TimestampEarlierThanTableFirstCommit(timeInMill, commitTs)
	} else if commit.timestamp < timeInMill && commit.version == latestVersion && !canReturnLastCommit {
		return nil, errno.TimestampLaterThanTableLastCommit(timeInMill, commitTs)
	}

	return commit, nil
}

func (h *historyManager) getEarliestDeltaFile() (int64, error) {
	version0 := filenames.DeltaFile(h.logStore.Root(), 0)
	iter, err := h.logStore.ListFrom(version0)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var earliestVersionOpt *store.FileMeta
	for v, err := iter.Next(); err == nil; v, err = iter.Next() {
		if filenames.IsDeltaFile(v.Path()) {
			earliestVersionOpt = v
			break
		}
	}
	if err != nil && err != io.EOF {
		return 0, err
	}
	if earliestVersionOpt == nil {
		return 0, errno.NoHistoryFound(h.logStore.Root())
	}
	return filenames.DeltaVersion(earliestVersionOpt.Path()), nil
}

func (h *historyManager) getEarliestReproducibleCommitVersion() (int64, error) {

	iter, err := h.logStore.ListFrom(filenames.DeltaFile(h.logStore.Root(), 0))
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var files []*store.FileMeta
	for f, err := iter.Next(); err == nil; f, err = iter.Next() {
		if filenames.IsCheckpointFile(f.Path()) || filenames.IsDeltaFile(f.Path()) {
			files = append(files, f)
		}
	}
	if err != nil && err != io.EOF {
		return 0, err
	}

	checkpointMap := make(map[tuple.T2[int64, int]]int)
	smallestDeltaVersion := int64(math.MaxInt64)
	lastCompleteCheckpoint := mo.None[int64]()

	for _, f := range files {

		nextFilePath := f.Path()
		if filenames.IsDeltaFile(nextFilePath) {
			version := filenames.DeltaVersion(nextFilePath)
			if version == 0 {
				return version, nil
			}
			smallestDeltaVersion = util.MinInt64(version, smallestDeltaVersion)
			if lastCompleteCheckpoint.IsPresent() && lastCompleteCheckpoint.MustGet() >= smallestDeltaVersion {
				return lastCompleteCheckpoint.MustGet(), nil
			}
		} else if filenames.IsCheckpointFile(nextFilePath) {

			checkpointVersion := filenames.CheckpointVersion(nextFilePath)
			parts := filenames.NumCheckpointParts(nextFilePath)
			if parts.IsAbsent() {
				lastCompleteCheckpoint = mo.Some(checkpointVersion)
			} else {
				numParts := parts.OrElse(1)
				key := tuple.New2(checkpointVersion, numParts)
				preCount := util.GetMapValue(checkpointMap, key, 0)
				if numParts == preCount+1 {
					lastCompleteCheckpoint = mo.Some(checkpointVersion)
				}
				checkpointMap[key] = preCount + 1
			}
		}
	}

	if lastCompleteCheckpoint.IsPresent() && lastCompleteCheckpoint.MustGet() >= smallestDeltaVersion {
		return lastCompleteCheckpoint.MustGet(), nil
	} else if smallestDeltaVersion < math.MaxInt64 {
		return 0, errno.NoReproducibleHistoryFound(h.logStore.Root())
	} else {
		return 0, errno.NoHistoryFound(h.logStore.Root())
	}
}

func (h *historyManager) getLastCommitBeforeTimestamp(commits []*commit, timeInMill int64) mo.Option[*commit] {
	var i int
	for i = len(commits) - 1; i >= 0; i-- {
		if commits[i].timestamp <= timeInMill {
			break
		}
	}

	if i < 0 {
		return mo.None[*commit]()
	}
	return mo.Some(commits[i])
}

func (h *historyManager) getCommits(logStore store.Store, logPath string, start int64, end int64) ([]*commit, error) {
	iter, err := logStore.ListFrom(filenames.DeltaFile(logPath, start))
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var commits []*commit
	for f, err := iter.Next(); err == nil; f, err = iter.Next() {
		if filenames.IsDeltaFile(f.Path()) {
			c := &commit{version: filenames.DeltaVersion(f.Path()), timestamp: f.TimeModified().UnixMilli()}
			if c.version < end {
				commits = append(commits, c)
			} else {
				break
			}
		}
	}
	if err != nil && err != io.EOF {
		return nil, err
	}

	return commits, nil
}

func (h *historyManager) monotonizeCommitTimestamps(commits []action.CommitMarker) []action.CommitMarker {
	i := 0
	length := len(commits)
	for i < length-1 {
		prevTimestamp := commits[i].GetTimestamp()
		if commits[i].GetVersion() > commits[i+1].GetVersion() {
			panic("Unordered commits provided.")
		}
		if prevTimestamp >= commits[i+1].GetTimestamp() {
			commits[i+1] = commits[i+1].WithTimestamp(prevTimestamp + 1)
		}
		i += 1
	}
	return commits
}

type commit struct {
	version   int64
	timestamp int64
}

func (c *commit) GetTimestamp() int64 {
	return c.timestamp
}

func (c *commit) WithTimestamp(timestamp int64) *commit {
	return &commit{
		version:   c.version,
		timestamp: timestamp,
	}
}

func (c *commit) GetVersion() int64 {
	return c.version
}
