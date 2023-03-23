package deltago

import (
	"time"

	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/store"
	"github.com/samber/mo"
)

type LogSegment struct {
	LogPath             string
	Version             int64
	Deltas              []*store.FileMeta
	Checkpoints         []*store.FileMeta
	CheckpointVersion   mo.Option[int64]
	LastCommitTimestamp time.Time
}

func (l *LogSegment) equal(other *LogSegment) bool {
	if other == nil {
		return false
	}
	if l.LogPath != other.LogPath ||
		l.Version != other.Version ||
		l.LastCommitTimestamp.Unix() != other.LastCommitTimestamp.Unix() {
		return false
	}
	if l.CheckpointVersion.OrEmpty() != other.CheckpointVersion.OrEmpty() {
		return false
	}
	if util.MustHash(l.Deltas) != util.MustHash(other.Deltas) {
		return false
	}
	if util.MustHash(l.Checkpoints) != util.MustHash(other.Checkpoints) {
		return false
	}
	return true
}

func emptyLogSegment(logPath string) *LogSegment {
	return &LogSegment{
		LogPath:             logPath,
		Version:             -1,
		Deltas:              nil,
		Checkpoints:         nil,
		CheckpointVersion:   mo.None[int64](),
		LastCommitTimestamp: time.Time{},
	}
}
