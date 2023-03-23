package deltago

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/barweiss/go-tuple"
	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/op"
	"github.com/csimplestring/delta-go/store"
	"github.com/csimplestring/delta-go/types"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/otiai10/copy"
	"github.com/repeale/fp-go"
	"github.com/stretchr/testify/assert"
)

func getTestLogDir(name string) string {
	path, err := filepath.Abs(fmt.Sprintf("./tests/golden/%s/_delta_log/", name))
	if err != nil {
		panic(err)
	}

	return "file://" + path + "/"
}

func getTestTableDir(name string) string {
	path, err := filepath.Abs(fmt.Sprintf("./tests/golden/%s/", name))
	if err != nil {
		panic(err)
	}

	return "file://" + path + "/"
}

func getTestConfig() Config {
	return Config{
		StorageConfig: StorageConfig{
			Scheme: Local,
		},
	}
}

func getTestTable(name string) (Log, error) {
	return ForTable(getTestTableDir(name),
		getTestConfig(),
		&SystemClock{})
}

func getTestEngineInfo() string {
	return "test-engine-info"
}

func getTestManualUpdate() *op.Operation {
	return &op.Operation{Name: op.MANUALUPDATE}
}

func getTestMetedata() *action.Metadata {
	st := types.NewStructType([]*types.StructField{types.NewStructField("x", &types.IntegerType{}, true)})
	schemaString, err := types.ToJSON(st)
	if err != nil {
		panic(err)
	}
	return &action.Metadata{SchemaString: schemaString}
}

func TestLog_snapshot(t *testing.T) {
	getDirDataFiles := func(tablePath string) []string {

		tablePath = strings.TrimPrefix(tablePath, "file:")

		stats, err := os.ReadDir(tablePath)
		assert.NoError(t, err)
		var res []string
		for _, file := range stats {
			if !file.IsDir() && strings.HasSuffix(file.Name(), "snappy.parquet") {
				res = append(res, file.Name())
			}
		}
		return res
	}

	verify := func(s Snapshot, expectedFiles []string, expectedVersion int64) {
		files, err := s.AllFiles()
		assert.NoError(t, err)
		assert.Equal(t, expectedVersion, s.Version())
		actual := fp.Map(func(f *action.AddFile) string { return f.Path })(files)
		sort.Strings(actual)
		sort.Strings(expectedFiles)
		assert.Equal(t, expectedFiles, actual)
	}

	table, err := getTestTable("snapshot-data0")
	assert.NoError(t, err)
	s, err := table.Snapshot()
	assert.NoError(t, err)
	data0_files := getDirDataFiles(getTestTableDir("snapshot-data0"))
	verify(s, data0_files, 0)

	table, err = getTestTable("snapshot-data1")
	assert.NoError(t, err)
	s, err = table.Snapshot()
	assert.NoError(t, err)
	data0_data1_files := getDirDataFiles(getTestTableDir("snapshot-data1"))
	verify(s, data0_data1_files, 1)

	table, err = getTestTable("snapshot-data2")
	assert.NoError(t, err)
	s, err = table.Snapshot()
	assert.NoError(t, err)
	var data2_files []string
	data0_data1_fileset := mapset.NewSet(data0_data1_files...)
	// we have overwritten files for data0 & data1; only data2 files should remain
	for _, f := range getDirDataFiles(getTestTableDir("snapshot-data2")) {
		if !data0_data1_fileset.Contains(f) {
			data2_files = append(data2_files, f)
		}
	}
	verify(s, data2_files, 2)

	table, err = getTestTable("snapshot-data3")
	assert.NoError(t, err)
	s, err = table.Snapshot()
	assert.NoError(t, err)
	var data2_data3_files []string
	for _, f := range getDirDataFiles(getTestTableDir("snapshot-data3")) {
		if !data0_data1_fileset.Contains(f) {
			data2_data3_files = append(data2_data3_files, f)
		}
	}
	verify(s, data2_data3_files, 3)

	table, err = getTestTable("snapshot-data2-deleted")
	assert.NoError(t, err)
	s, err = table.Snapshot()
	assert.NoError(t, err)
	var data3_files []string
	data2_fileset := mapset.NewSet(data2_files...)
	for _, f := range getDirDataFiles(getTestTableDir("snapshot-data2-deleted")) {
		if !data0_data1_fileset.Contains(f) && !data2_fileset.Contains(f) {
			data3_files = append(data3_files, f)
		}
	}
	verify(s, data3_files, 4)

	table, err = getTestTable("snapshot-repartitioned")
	assert.NoError(t, err)
	s, err = table.Snapshot()
	assert.NoError(t, err)
	allFiles, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allFiles))
	assert.Equal(t, int64(5), s.Version())

	table, err = getTestTable("snapshot-vacuumed")
	assert.NoError(t, err)
	s, err = table.Snapshot()
	assert.NoError(t, err)
	verify(s, getDirDataFiles(getTestTableDir("snapshot-vacuumed")), 5)
}

func TestLog_checkpoint(t *testing.T) {
	table, err := getTestTable("checkpoint")
	assert.NoError(t, err)

	snapshot, err := table.Snapshot()
	assert.NoError(t, err)
	assert.NotNil(t, snapshot)

	allFiles, err := snapshot.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, "15", allFiles[0].Path)
	assert.Equal(t, int64(14), snapshot.Version())

	scan, err := snapshot.Scan(nil)
	assert.NoError(t, err)
	assert.NotNil(t, scan)

	iter, err := scan.Files()
	assert.NoError(t, err)
	assert.NotNil(t, iter)
	defer iter.Close()

	var res []string
	for iter.Next() {
		f, err := iter.Value()
		assert.NoError(t, err)
		res = append(res, f.Path)
	}
	iter.Close()
	assert.Equal(t, "15", res[0])
}

func TestLog_updateDeletedDir(t *testing.T) {
	srcTableDir := getTestTableDir("update-deleted-directory")
	srcTableDir = strings.TrimPrefix(srcTableDir, "file://")
	destTableDir, err := os.MkdirTemp(os.TempDir(), "deltago")
	assert.NoError(t, err)
	defer os.RemoveAll(destTableDir)

	err = copy.Copy(srcTableDir, destTableDir)
	assert.NoError(t, err)

	table, err := ForTable("file://"+destTableDir,
		getTestConfig(),
		&SystemClock{})
	assert.NoError(t, err)

	err = os.RemoveAll(destTableDir)
	assert.NoError(t, err)

	s, err := table.Update()
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), s.Version())
}

func TestLog_update_should_not_pick_up_delta_files_earlier_than_checkpoint(t *testing.T) {
	engineInfo := "test-engine-info"
	manualUpdate := &op.Operation{Name: op.MANUALUPDATE}
	metadata := getTestMetedata()

	destTableDir, err := os.MkdirTemp(os.TempDir(), "deltago")
	assert.NoError(t, err)
	defer os.RemoveAll(destTableDir)

	err = os.Mkdir(destTableDir+"/_delta_log", os.ModePerm)
	assert.NoError(t, err)

	table1, err := ForTable("file://"+destTableDir,
		getTestConfig(),
		&SystemClock{})
	assert.NoError(t, err)

	for i := 1; i <= 5; i++ {
		txn, err := table1.StartTransaction()
		assert.NoError(t, err)

		var filesToCommit []action.Action
		file := &action.AddFile{Path: fmt.Sprintf("%d", i), PartitionValues: map[string]string{}, Size: 1, ModificationTime: 1, DataChange: true}
		if i > 1 {
			now := time.Now().UnixMilli()
			delete := &action.RemoveFile{Path: fmt.Sprintf("%d", i-1), DeletionTimestamp: &now, DataChange: true}
			filesToCommit = append(filesToCommit, delete)
		}

		filesToCommit = append(filesToCommit, file)

		if i == 1 {
			err := txn.UpdateMetadata(metadata)
			assert.NoError(t, err)
		}
		_, err = txn.Commit(iter.FromSlice(filesToCommit), manualUpdate, engineInfo)
		assert.NoError(t, err)
	}

	table2, err := ForTable("file://"+destTableDir,
		getTestConfig(),
		&SystemClock{})
	assert.NoError(t, err)

	for i := 6; i <= 15; i++ {
		txn, err := table1.StartTransaction()
		assert.NoError(t, err)

		file := &action.AddFile{Path: fmt.Sprintf("%d", i), PartitionValues: map[string]string{}, Size: 1, ModificationTime: 1, DataChange: true}
		now := time.Now().UnixMilli()
		delete := &action.RemoveFile{Path: fmt.Sprintf("%d", i-1), DeletionTimestamp: &now, DataChange: true}

		filesToCommit := []action.Action{delete, file}

		_, err = txn.Commit(iter.FromSlice(filesToCommit), manualUpdate, engineInfo)
		assert.NoError(t, err)
	}

	// Since log2 is a separate instance, it shouldn't be updated to version 15
	s2, err := table2.Snapshot()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), s2.Version())

	updatedS2, err := table2.Update()
	assert.NoError(t, err)

	s1, err := table1.Snapshot()
	assert.NoError(t, err)
	assert.Equal(t, s1.Version(), updatedS2.Version(), "Did not update to correct version")

	deltas := table2.(*logImpl).snapshotReader.snapshot().logSegment.Deltas
	assert.Equal(t, 4, len(deltas), "Expected 4 files starting at version 11 to 14")

	versions := fp.Map(func(f *store.FileMeta) int64 { return filenames.DeltaVersion(f.Path()) })(deltas)
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	assert.Equal(t, []int64{11, 12, 13, 14}, versions, "Received the wrong files for update")
}

func TestLog_handle_corrupted_last_checkpoint_file(t *testing.T) {
	srcTableDir := getTestTableDir("corrupted-last-checkpoint")
	srcTableDir = strings.TrimPrefix(srcTableDir, "file://")
	destTableDir, err := os.MkdirTemp(os.TempDir(), "deltago")
	assert.NoError(t, err)
	defer os.RemoveAll(destTableDir)

	err = copy.Copy(srcTableDir, destTableDir)
	assert.NoError(t, err)

	table, err := ForTable("file://"+destTableDir,
		getTestConfig(),
		&SystemClock{})
	assert.NoError(t, err)

	logImpl1 := table.(*logImpl)
	lc, err := LastCheckpoint(logImpl1.store)
	assert.NoError(t, err)
	assert.True(t, lc.IsPresent())

	lastcheckpoint1 := lc.MustGet()

	fs, err := store.GetFileSystem(logImpl1.logPath + LastCheckpointPath)
	assert.NoError(t, err)

	err = fs.Create(logImpl1.logPath+LastCheckpointPath, true)
	assert.NoError(t, err)

	table2, err := ForTable("file://"+destTableDir,
		getTestConfig(),
		&SystemClock{})
	assert.NoError(t, err)

	lc2, err := LastCheckpoint(table2.(*logImpl).store)
	lastcheckpoint2 := lc2.MustGet()

	assert.NoError(t, err)
	assert.Equal(t, FromMetadata(*lastcheckpoint2), FromMetadata(*lastcheckpoint1))
}

func TestLog_paths_should_be_canonicalized_normal_characters(t *testing.T) {
	dataPath := getTestTableDir("canonicalized-paths-normal-a")
	log, err := ForTable(dataPath, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	s, err := log.Update()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), s.Version())

	files, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(files))

	dataPath = getTestTableDir("canonicalized-paths-normal-b")
	log, err = ForTable(dataPath, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	s, err = log.Update()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), s.Version())

	files, err = s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(files))
}

func TestLog_paths_should_be_canonicalized_special_characters(t *testing.T) {
	dataPath := getTestTableDir("canonicalized-paths-special-a")
	log, err := ForTable(dataPath, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	s, err := log.Update()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), s.Version())

	files, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(files))

	dataPath = getTestTableDir("canonicalized-paths-special-b")
	log, err = ForTable(dataPath, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	s, err = log.Update()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), s.Version())

	files, err = s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(files))
}

func TestLog_do_not_relative_path_in_remove_files(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "delta")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dataPath := "file://" + tempDir

	log, err := ForTable(dataPath, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	logPath := tempDir + "/_delta_log"
	err = os.Mkdir(tempDir+"/_delta_log", os.ModePerm)
	assert.NoError(t, err)
	assert.DirExists(t, logPath)

	path := tempDir + "/a/b/c"

	now := time.Now().UnixMilli()
	size := int64(0)
	metadata := getTestMetedata()

	removeFile := &action.RemoveFile{
		Path:                 path,
		DeletionTimestamp:    &now,
		DataChange:           true,
		ExtendedFileMetadata: false,
		PartitionValues:      nil,
		Size:                 &size,
		Tags:                 nil,
	}
	actions := iter.FromSlice([]action.Action{removeFile, metadata})

	trx, err := log.StartTransaction()
	assert.NoError(t, err)

	_, err = trx.Commit(actions, getTestManualUpdate(), getTestEngineInfo())
	assert.NoError(t, err)

	s, err := log.Update()
	assert.NoError(t, err)
	commitedRemove, err := s.(*snapshotImp).tombstones()
	assert.NoError(t, err)
	assert.Equal(t, "file://"+path, commitedRemove[0].Path)
}

func TestLog_delete_and_readd_the_same_file_in_different_transactions(t *testing.T) {
	log, err := ForTable(getTestTableDir("delete-re-add-same-file-different-transactions"), getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	s, err := log.Snapshot()
	assert.NoError(t, err)
	files, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(files))

	actualRes := mapset.NewSet(fp.Map(func(a *action.AddFile) string { return a.Path })(files)...)
	assert.True(t, actualRes.Equal(mapset.NewSet("foo", "bar")))

	foo := fp.Filter(func(af *action.AddFile) bool { return af.Path == "foo" })(files)
	assert.Equal(t, int64(1700000000000), foo[0].ModificationTime)
}

func TestLog_version_not_continuous(t *testing.T) {
	_, err := ForTable(getTestTableDir("versions-not-contiguous"), getTestConfig(), &SystemClock{})
	assert.ErrorIs(t, err, errno.DeltaVersionNotContinuous([]int64{0, 2}))
}

func TestLog_state_reconstruction_without_action_should_fail(t *testing.T) {
	for _, name := range []string{"protocol", "metadata"} {
		_, err := ForTable(getTestTableDir(fmt.Sprintf("deltalog-state-reconstruction-without-%s", name)), getTestConfig(), &SystemClock{})
		assert.ErrorIs(t, err, errno.ActionNotFound(name, 0))
	}
}

func TestLog_state_reconstruction_from_checkpoint_with_missing_action_should_fail(t *testing.T) {
	for _, name := range []string{"protocol", "metadata"} {
		_, err := ForTable(getTestTableDir(fmt.Sprintf("deltalog-state-reconstruction-from-checkpoint-missing-%s", name)), getTestConfig(), &SystemClock{})
		assert.ErrorIs(t, err, errno.ActionNotFound(name, 10))
	}
}

func TestLog_table_protocol_version_greater_than_client_reader_protocol_version(t *testing.T) {
	_, err := ForTable(getTestTableDir("deltalog-invalid-protocol-version"), getTestConfig(), &SystemClock{})
	assert.ErrorIs(t, err, errno.InvalidProtocolVersionError())
}

func TestLog_get_commit_info(t *testing.T) {
	log, err := ForTable(getTestTableDir("deltalog-commit-info"), getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	ci, err := log.CommitInfoAt(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), *ci.Version)
	assert.Equal(t, int64(1540415658000), ci.Timestamp)
	assert.Equal(t, "user_0", *ci.UserID)
	assert.Equal(t, "username_0", *ci.UserName)
	assert.Equal(t, "WRITE", ci.Operation)
	assert.Equal(t, map[string]string{"test": "test"}, ci.OperationParameters)
	assert.Equal(t, &action.JobInfo{JobID: "job_id_0", JobName: "job_name_0", RunId: "run_id_0", JobOwnerId: "job_owner_0", TriggerType: "trigger_type_0"}, ci.Job)
	assert.Equal(t, &action.NotebookInfo{NotebookId: "notebook_id_0"}, ci.Notebook)
	assert.Equal(t, "cluster_id_0", *ci.ClusterId)
	assert.Equal(t, int64(-1), *ci.ReadVersion)
	assert.Equal(t, "default", *ci.IsolationLevel)
	assert.Equal(t, true, *ci.IsBlindAppend)
	assert.Equal(t, map[string]string{"test": "test"}, ci.OperationMetrics)
	assert.Equal(t, "foo", *ci.UserMetadata)

	// use an actual spark transaction example
	log, err = ForTable(getTestTableDir("snapshot-vacuumed"), getTestConfig(), &SystemClock{})
	assert.NoError(t, err)
	for i := 0; i <= 5; i++ {
		ci, err = log.CommitInfoAt(int64(i))
		assert.NoError(t, err)
		assert.Equal(t, int64(i), *ci.Version)
		if i > 0 {
			assert.Equal(t, int64(i-1), *ci.ReadVersion)
		}
	}

	_, err = log.CommitInfoAt(99)
	assert.Error(t, err)
}

func TestLog_getChanges_no_data_loss(t *testing.T) {

	versionToActionsMap := map[int64][]string{
		0: {"CommitInfo", "Protocol", "Metadata", "AddFile"},
		1: {"CommitInfo", "AddCDCFile", "RemoveFile"},
		2: {"CommitInfo", "Protocol", "SetTransaction"},
	}

	verifyChanges := func(log Log, startVersion int64) {
		changes, err := log.Changes(startVersion, false)
		assert.NoError(t, err)
		versionLogs, err := iter.ToSlice(changes)
		assert.NoError(t, err)
		assert.Equal(t, 3-startVersion, int64(len(versionLogs)))

		var versionsInOrder []int64
		for _, versionLog := range versionLogs {
			version := versionLog.Version()
			actions, err := versionLog.Actions()
			assert.NoError(t, err)

			actionNames := fp.Map(func(a action.Action) string {
				return strings.TrimPrefix(reflect.TypeOf(a).String(), "*action.")
			})(actions)
			expectedActions := versionToActionsMap[version]
			assert.Equal(t, expectedActions, actionNames)

			versionsInOrder = append(versionsInOrder, version)
		}

		j := 0
		for i := startVersion; i <= 2; i++ {
			assert.Equal(t, int64(i), versionsInOrder[j])
			j++
		}
	}

	log, err := ForTable(getTestTableDir("deltalog-getChanges"), getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	// standard cases
	verifyChanges(log, 0)
	verifyChanges(log, 1)
	verifyChanges(log, 2)

	// non-existant start version
	versionLogIter, err := log.Changes(3, false)
	assert.NoError(t, err)
	assert.False(t, versionLogIter.Next())

	// negative start version
	_, err = log.Changes(-1, false)
	assert.ErrorIs(t, err, errno.ErrIllegalArgument)
}

func TestLog_getChanges_data_loss(t *testing.T) {
	tablePath := getTestTableDir("deltalog-getChanges")
	tempDir, err := os.MkdirTemp("", "delta")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	err = copy.Copy(strings.TrimPrefix(tablePath, "file://"), tempDir)
	assert.NoError(t, err)

	log, err := ForTable("file://"+tempDir, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	// delete 2 files
	err = os.Remove(tempDir + "/_delta_log/00000000000000000000.json")
	assert.NoError(t, err)
	err = os.Remove(tempDir + "/_delta_log/00000000000000000001.json")
	assert.NoError(t, err)

	vlIter, err := log.Changes(0, false)
	assert.NoError(t, err)
	versionLogs, err := iter.ToSlice(vlIter)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(versionLogs))

	_, err = log.Changes(0, true)
	assert.ErrorIs(t, err, errno.ErrIllegalState)
}

func TestLog_table_exists(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "delta")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	log, err := ForTable("file://"+tempDir, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)
	assert.False(t, log.TableExists())

	trx, err := log.StartTransaction()
	assert.NoError(t, err)

	_, err = trx.Commit(iter.FromSlice([]action.Action{
		getTestMetedata(),
	}), &op.Operation{Name: op.CREATETABLE}, getTestEngineInfo())
	assert.NoError(t, err)

	assert.True(t, log.TableExists())
}

func TestLog_schema_must_contain_all_partition_columns(t *testing.T) {

	schema := types.NewStructType([]*types.StructField{
		types.NewStructField("a", &types.StringType{}, true),
		types.NewStructField("b", &types.LongType{}, true),
		types.NewStructField("foo", &types.IntegerType{}, true),
		types.NewStructField("bar", &types.BooleanType{}, true),
	})
	schemaString, err := types.ToJSON(schema)
	assert.NoError(t, err)

	inputs := []tuple.T2[[]string, []string]{
		tuple.New2([]string{"a", "b"}, []string{}),
		tuple.New2([]string{}, []string{}),
		tuple.New2([]string{"a", "b", "c", "d"}, []string{"c", "d"}),
	}

	for _, i := range inputs {
		inputPartCols := i.V1
		missingPartCols := i.V2

		dir, err := os.MkdirTemp("", "delta")
		assert.NoError(t, err)
		defer os.RemoveAll(dir)

		log, err := ForTable("file://"+dir, getTestConfig(), &SystemClock{})
		assert.NoError(t, err)

		metadata := &action.Metadata{
			SchemaString:     schemaString,
			PartitionColumns: inputPartCols,
		}

		trx, err := log.StartTransaction()
		assert.NoError(t, err)
		if len(missingPartCols) > 0 {
			assert.ErrorIs(t, trx.UpdateMetadata(metadata), errno.ErrDeltaStandalone)
		}
	}
}

func TestLog_schema_contains_no_data_columns_and_only_partition_columns(t *testing.T) {
	dir, err := os.MkdirTemp("", "delta")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	log, err := ForTable("file://"+dir, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	schema := types.NewStructType([]*types.StructField{
		types.NewStructField("part_1", &types.StringType{}, true),
		types.NewStructField("part_2", &types.LongType{}, true),
	})
	schemaString, err := types.ToJSON(schema)
	assert.NoError(t, err)

	metadata := &action.Metadata{
		SchemaString:     schemaString,
		PartitionColumns: []string{"part_1", "part_2"},
	}

	trx, err := log.StartTransaction()
	assert.NoError(t, err)
	assert.ErrorIs(t, trx.UpdateMetadata(metadata), errno.ErrDeltaStandalone)
}

func TestLog_getVersionBeforeOrAtTimestamp_and_getVersionAtOrAfterTimestamp(t *testing.T) {
	dir, err := os.MkdirTemp("", "delta")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	log, err := ForTable("file://"+dir, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	// ========== case 0: delta table is empty ==========
	v, err := log.VersionBeforeOrAtTimestamp(time.Now().UnixMilli())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), v)
	v, err = log.VersionAtOrAfterTimestamp(time.Now().UnixMilli())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), v)

	for i := 0; i <= 2; i++ {
		trx, err := log.StartTransaction()
		assert.NoError(t, err)
		if i == 0 {
			assert.NoError(t, trx.UpdateMetadata(getTestMetedata()))
		}
		files := []action.Action{
			&action.AddFile{Path: strconv.Itoa(i), PartitionValues: map[string]string{}, Size: 1, ModificationTime: 1, DataChange: true},
		}
		_, err = trx.Commit(iter.FromSlice(files), getTestManualUpdate(), getTestEngineInfo())
		assert.NoError(t, err)
	}

	logPath := dir + "/_delta_log/"
	delta0 := filenames.DeltaFile(logPath, 0)
	delta1 := filenames.DeltaFile(logPath, 1)
	delta2 := filenames.DeltaFile(logPath, 2)

	setLastModified := func(path string, ts int64) {
		os.Chtimes(path, time.Now(), time.UnixMilli(ts))
	}

	setLastModified(delta0, 1000)
	setLastModified(delta1, 2000)
	setLastModified(delta2, 3000)

	assertGetVersion := func(fn func(int64) (int64, error), input int64, expected int64) {
		actual, err := fn(input)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
	// ========== case 1: before first commit ==========
	_, err = log.VersionBeforeOrAtTimestamp(500)
	assert.ErrorIs(t, err, errno.ErrIllegalArgument)
	assertGetVersion(log.VersionAtOrAfterTimestamp, 500, 0)

	// ========== case 2: at first commit ==========
	assertGetVersion(log.VersionBeforeOrAtTimestamp, 1000, 0)
	assertGetVersion(log.VersionAtOrAfterTimestamp, 1000, 0)

	// ========== case 3: between two normal commits ==========
	assertGetVersion(log.VersionBeforeOrAtTimestamp, 1500, 0)
	assertGetVersion(log.VersionAtOrAfterTimestamp, 1500, 1)

	// // ========== case 4: at last commit ==========
	assertGetVersion(log.VersionBeforeOrAtTimestamp, 3000, 2)
	assertGetVersion(log.VersionAtOrAfterTimestamp, 3000, 2)

	// ========== case 5: after last commit ==========
	assertGetVersion(log.VersionBeforeOrAtTimestamp, 4000, 2)
	_, err = log.VersionAtOrAfterTimestamp(4000)
	assert.ErrorIs(t, err, errno.ErrIllegalArgument)
}

func TestLog_getVersionBeforeOrAtTimestamp_and_getVersionAtOrAfterTimestamp_recoverability(t *testing.T) {
	dir, err := os.MkdirTemp("", "delta")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	logPath := dir + "/_delta_log/"
	err = os.Mkdir(logPath, os.ModePerm)
	assert.NoError(t, err)

	log, err := ForTable("file://"+dir, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	for i := 0; i <= 35; i++ {
		trx, err := log.StartTransaction()
		assert.NoError(t, err)
		if i == 0 {
			trx.UpdateMetadata(getTestMetedata())
		}
		files := []action.Action{
			&action.AddFile{Path: strconv.Itoa(i), PartitionValues: map[string]string{}, Size: 1, ModificationTime: 1, DataChange: true},
		}
		_, err = trx.Commit(iter.FromSlice(files), getTestManualUpdate(), getTestEngineInfo())
		assert.NoError(t, err)
	}
	nowEpochMs := time.Now().UnixMilli() / 1000 * 1000

	for i := 0; i <= 35; i++ {
		delta := filenames.DeltaFile(logPath, int64(i))
		if i >= 25 {
			assert.NoError(t, os.Chtimes(delta, time.Now(), time.UnixMilli(nowEpochMs+int64(i*1000))))
		} else {
			assert.NoError(t, os.Remove(delta))
		}
	}

	assertGetVersion := func(fn func(int64) (int64, error), input int64, expected int64) {
		actual, err := fn(input)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	}
	// A checkpoint exists at version 30, so all versions [30, 35] are recoverable.
	// Nonetheless, getVersionBeforeOrAtTimestamp and getVersionAtOrAfterTimestamp do not
	// require that the version is recoverable, so we should still be able to get back versions
	// [25-29]
	for i := 25; i <= 34; i++ {
		if i == 25 {
			_, err = log.VersionBeforeOrAtTimestamp(nowEpochMs + int64(i*1000-1))
			assert.ErrorIs(t, err, errno.ErrIllegalArgument)
		} else {
			assertGetVersion(log.VersionBeforeOrAtTimestamp, nowEpochMs+int64(i*1000-1), int64(i-1))
		}

		assertGetVersion(log.VersionAtOrAfterTimestamp, nowEpochMs+int64(i*1000-1), int64(i))
		assertGetVersion(log.VersionBeforeOrAtTimestamp, nowEpochMs+int64(i*1000), int64(i))
		assertGetVersion(log.VersionAtOrAfterTimestamp, nowEpochMs+int64(i*1000), int64(i))
		assertGetVersion(log.VersionBeforeOrAtTimestamp, nowEpochMs+int64(i*1000+1), int64(i))

		if i == 35 {
			log.VersionAtOrAfterTimestamp(nowEpochMs + int64(i*1000+1))
		} else {
			assertGetVersion(log.VersionAtOrAfterTimestamp, nowEpochMs+int64(i*1000+1), int64(i+1))
		}
	}
}
