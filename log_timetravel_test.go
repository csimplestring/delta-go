package deltago

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/otiai10/copy"
	"github.com/repeale/fp-go"
	"github.com/stretchr/testify/assert"
)

func getTestDirDataFile(tablePath string) []string {
	tablePath = strings.TrimPrefix(tablePath, "file://")
	entries, err := os.ReadDir(tablePath)
	if err != nil {
		panic(err)
	}
	var res []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), "snappy.parquet") {
			res = append(res, entry.Name())
		}
	}
	return res
}

type timeTravelFixture struct {
	start             int64
	dataFilesVersion0 []string
	dataFilesVersion1 []string
	dataFilesVersion2 []string
}

func newTimeTravelFixture() *timeTravelFixture {

	v0 := getTestDirDataFile(getTestFileDir("time-travel-start"))
	v1 := getTestDirDataFile(getTestFileDir("time-travel-start-start20"))
	v2 := getTestDirDataFile(getTestFileDir("time-travel-start-start20-start40"))

	return &timeTravelFixture{
		start:             1540415658000,
		dataFilesVersion0: v0,
		dataFilesVersion1: v1,
		dataFilesVersion2: v2,
	}
}

func (f *timeTravelFixture) verifyTimeTravelSnapshot(t *testing.T, s Snapshot, expectedFiles []string, expectedVersion int64) {
	assert.Equal(t, expectedVersion, s.Version())
	files, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, len(expectedFiles), len(files))

	inEpectedFiles := fp.Every(func(af *action.AddFile) bool {
		return util.Exists(expectedFiles, func(e string) bool {
			return af.Path == e
		})
	})(files)
	assert.True(t, inEpectedFiles)
}

func TestLog_time_travel_versionAsOf(t *testing.T) {
	srcTableDir := getTestFileDir("time-travel-start-start20-start40")
	srcTableDir = strings.TrimPrefix(srcTableDir, "file://")
	destTableDir, err := os.MkdirTemp("", "deltago")
	assert.NoError(t, err)
	defer os.RemoveAll(destTableDir)

	err = copy.Copy(srcTableDir, destTableDir)
	assert.NoError(t, err)

	log, err := ForTable("file://"+destTableDir, getTestFileConfig(), &SystemClock{})
	assert.NoError(t, err)

	fixture := newTimeTravelFixture()
	s0, err := log.SnapshotForVersionAsOf(0)
	assert.NoError(t, err)
	s1, err := log.SnapshotForVersionAsOf(1)
	assert.NoError(t, err)
	s2, err := log.SnapshotForVersionAsOf(2)
	assert.NoError(t, err)

	// Correct cases
	fixture.verifyTimeTravelSnapshot(t, s0, fixture.dataFilesVersion0, 0)
	fixture.verifyTimeTravelSnapshot(t, s1, fixture.dataFilesVersion1, 1)
	fixture.verifyTimeTravelSnapshot(t, s2, fixture.dataFilesVersion2, 2)

	// Error case - version after latest commit
	_, err = log.SnapshotForVersionAsOf(3)
	assert.ErrorIs(t, err, errno.VersionNotExist(3, 0, 2))

	// Error case - version before earliest commit
	_, err = log.SnapshotForVersionAsOf(-1)
	assert.ErrorIs(t, err, errno.VersionNotExist(-1, 0, 2))

	f := filenames.DeltaFile(destTableDir+"/_delta_log/", 0)

	err = os.Remove(f)
	assert.NoError(t, err)
	_, err = log.SnapshotForVersionAsOf(0)
	assert.ErrorIs(t, err, errno.NoReproducibleHistoryFound(""))

}

func TestLog_timestampAsOf_with_timestamp_in_between_commits_should_use_commit_before_timestamp(t *testing.T) {
	f := newTimeTravelFixture()

	tablePath := getTestFileDir("time-travel-start-start20-start40")
	logDir := strings.TrimPrefix(tablePath, "file://") + "_delta_log/"
	err := os.Chtimes(logDir+"00000000000000000000.json", time.Now(), time.UnixMilli(f.start))
	assert.NoError(t, err)
	err = os.Chtimes(logDir+"00000000000000000001.json", time.Now(), time.UnixMilli(f.start).Add(time.Minute*20))
	assert.NoError(t, err)
	err = os.Chtimes(logDir+"00000000000000000002.json", time.Now(), time.UnixMilli(f.start).Add(time.Minute*40))
	assert.NoError(t, err)

	log, err := getTestFileTable("time-travel-start-start20-start40")
	assert.NoError(t, err)

	s, err := log.SnapshotForTimestampAsOf(time.UnixMilli(f.start).Add(time.Minute * 10).UnixMilli())
	assert.NoError(t, err)
	f.verifyTimeTravelSnapshot(t, s, f.dataFilesVersion0, 0)

	s, err = log.SnapshotForTimestampAsOf(time.UnixMilli(f.start).Add(time.Minute * 30).UnixMilli())
	assert.NoError(t, err)
	f.verifyTimeTravelSnapshot(t, s, f.dataFilesVersion1, 1)
}

func TestLog_timestampAsOf_with_timestamp_after_last_commit_should_fail(t *testing.T) {
	f := newTimeTravelFixture()

	tablePath := getTestFileDir("time-travel-start-start20-start40")
	logDir := strings.TrimPrefix(tablePath, "file://") + "_delta_log/"
	err := os.Chtimes(logDir+"00000000000000000000.json", time.Now(), time.UnixMilli(f.start))
	assert.NoError(t, err)
	err = os.Chtimes(logDir+"00000000000000000001.json", time.Now(), time.UnixMilli(f.start).Add(time.Minute*20))
	assert.NoError(t, err)
	err = os.Chtimes(logDir+"00000000000000000002.json", time.Now(), time.UnixMilli(f.start).Add(time.Minute*40))
	assert.NoError(t, err)

	log, err := getTestFileTable("time-travel-start-start20-start40")
	assert.NoError(t, err)

	_, err = log.SnapshotForTimestampAsOf(time.UnixMilli(f.start).Add(time.Minute * 50).UnixMilli())
	assert.ErrorIs(t, err, errno.ErrIllegalArgument)
	latest := time.UnixMilli(f.start).Add(time.Minute * 40).UnixMilli()
	usr := time.UnixMilli(f.start).Add(time.Minute * 50).UnixMilli()
	assert.Equal(t, errno.TimestampLaterThanTableLastCommit(usr, latest).Error(), err.Error())
}

func TestLog_timestampAsOf_with_timestamp_on_exact_commit_timestamp(t *testing.T) {
	f := newTimeTravelFixture()

	tablePath := getTestFileDir("time-travel-start-start20-start40")
	logDir := strings.TrimPrefix(tablePath, "file://") + "_delta_log/"
	err := os.Chtimes(logDir+"00000000000000000000.json", time.Now(), time.UnixMilli(f.start))
	assert.NoError(t, err)
	err = os.Chtimes(logDir+"00000000000000000001.json", time.Now(), time.UnixMilli(f.start).Add(time.Minute*20))
	assert.NoError(t, err)
	err = os.Chtimes(logDir+"00000000000000000002.json", time.Now(), time.UnixMilli(f.start).Add(time.Minute*40))
	assert.NoError(t, err)

	log, err := getTestFileTable("time-travel-start-start20-start40")
	assert.NoError(t, err)

	s, err := log.SnapshotForTimestampAsOf(time.UnixMilli(f.start).UnixMilli())
	assert.NoError(t, err)
	f.verifyTimeTravelSnapshot(t, s, f.dataFilesVersion0, 0)

	s, err = log.SnapshotForTimestampAsOf(time.UnixMilli(f.start).Add(time.Minute * 20).UnixMilli())
	assert.NoError(t, err)
	f.verifyTimeTravelSnapshot(t, s, f.dataFilesVersion1, 1)

	s, err = log.SnapshotForTimestampAsOf(time.UnixMilli(f.start).Add(time.Minute * 40).UnixMilli())
	assert.NoError(t, err)
	f.verifyTimeTravelSnapshot(t, s, f.dataFilesVersion2, 2)
}

func TestLog_time_travel_with_schema_changes_should_instantiate_old_schema(t *testing.T) {
	tablePath := getTestFileDir("time-travel-schema-changes-a")
	orig_schema_data_files := getTestDirDataFile(tablePath)

	log, err := getTestFileTable("time-travel-schema-changes-b")
	assert.NoError(t, err)

	f := newTimeTravelFixture()
	s, err := log.SnapshotForVersionAsOf(0)
	assert.NoError(t, err)
	f.verifyTimeTravelSnapshot(t, s, orig_schema_data_files, 0)
}

func TestLog_time_travel_with_partition_changes_should_instantiate_old_schema(t *testing.T) {

	getPartitionDirDataFiles := func(tablePath string) []string {
		entries, err := os.ReadDir(tablePath)
		assert.NoError(t, err)
		var res []string
		for _, entry := range entries {
			if entry.IsDir() {
				subEntries, err := os.ReadDir(tablePath + "/" + entry.Name())
				assert.NoError(t, err)
				for _, se := range subEntries {
					if !se.IsDir() && strings.HasSuffix(se.Name(), "snappy.parquet") {
						res = append(res, se.Name())
					}
				}
			}
		}
		return res
	}

	// write data to a table with some original partition
	tablePath := getTestFileDir("time-travel-partition-changes-a")
	orig_partition_data_files := getPartitionDirDataFiles(strings.TrimPrefix(tablePath, "file://"))

	// then append more data to that "same" table using a different partition
	// reading version 0 should show only the original partition data files

	log, err := getTestFileTable("time-travel-partition-changes-b")
	assert.NoError(t, err)

	s, err := log.SnapshotForVersionAsOf(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), s.Version())

	files, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, len(orig_partition_data_files), len(files))

	ok := fp.Every(func(af *action.AddFile) bool {
		return util.Exists(orig_partition_data_files, func(e string) bool {
			return strings.Contains(af.Path, e)
		})
	})(files)
	assert.True(t, ok)
}
