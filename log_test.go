package deltago

import (
	"fmt"
	"io"
	"net/url"
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
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/csimplestring/delta-go/internal/util/path"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/op"
	"github.com/csimplestring/delta-go/store"
	"github.com/csimplestring/delta-go/types"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/repeale/fp-go"
	"github.com/rotisserie/eris"
	"github.com/stretchr/testify/assert"
)

// File log store tests

func getTestFileDir(name string) string {
	path, err := filepath.Abs(fmt.Sprintf("./tests/golden/%s/", name))
	if err != nil {
		panic(err)
	}

	return "file://" + path + "/"
}

func getTestFileBaseDir() string {
	path, err := filepath.Abs("./tests/golden")
	if err != nil {
		panic(err)
	}
	return "file://" + path + "/"
}

func getTestFileConfig() Config {
	return Config{
		StoreType: "file",
	}
}

// func getTestFileTable(name string) (Log, error) {
// 	return ForTable(getTestFileDir(name),
// 		getTestFileConfig(),
// 		&SystemClock{})
// }

// Azure Blob log store tests

func getTestAzBlobBaseDir() string {
	os.Setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
	os.Setenv("AZURE_STORAGE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

	return "azblob://golden"
}

func getTestAzBlobConfig() Config {
	return Config{
		StoreType: "azblob",
	}
}

// Google Cloud Storage log store tests

func getTestGoogleStorageBaseDir() string {
	return "gs://golden"
}

func getTestGoogleStorageConfig() Config {
	return Config{
		StoreType: "gs",
	}
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

type testLogCase struct {
	name string

	blobDir      *util.BlobDir
	urlstr       string
	config       Config
	dataPathBase string

	//
	tempFile    string
	tempDir     string
	copiedFiles []string
	copiedDir   string
}

func (t *testLogCase) getLog(name string) (Log, error) {
	dataPath := fmt.Sprintf("%s/%s", t.dataPathBase, name)

	return ForTable(dataPath, t.config, &SystemClock{})
}

func (t *testLogCase) getTempLog() (Log, error) {

	tempDir, tempFile, err := t.blobDir.CreateTemp()
	if err != nil {
		return nil, err
	}

	t.tempDir = tempDir
	t.tempFile = tempFile

	base := strings.TrimSuffix(t.dataPathBase, "/")
	dir := strings.TrimSuffix(strings.TrimPrefix(tempDir, "/"), "/")
	dataPath := fmt.Sprintf("%s/%s", base, dir)

	return ForTable(dataPath, t.config, &SystemClock{})
}

func (t *testLogCase) copyLog(name string) (Log, error) {
	dir, files, err := t.blobDir.Copy(name)
	if err != nil {
		return nil, err
	}

	t.copiedDir = dir
	t.copiedFiles = files

	dataPath := fmt.Sprintf("%s/%s", t.dataPathBase, dir)

	return ForTable(dataPath, t.config, &SystemClock{})
}

func (t *testLogCase) cleanTempDir() {
	if len(t.tempDir) != 0 {
		if err := t.blobDir.Delete(t.tempDir, []string{t.tempFile}, true); err != nil {
			panic(err)
		}
	}
	if len(t.copiedFiles) != 0 {
		if err := t.blobDir.Delete(t.copiedDir, t.copiedFiles, true); err != nil {
			panic(err)
		}
	}
}

func (t *testLogCase) clean() {
	if len(t.tempDir) != 0 {
		if err := t.blobDir.Delete(t.tempDir, []string{t.tempFile}, true); err != nil {
			panic(err)
		}
	}
	if len(t.copiedFiles) != 0 {
		if err := t.blobDir.Delete(t.copiedDir, t.copiedFiles, true); err != nil {
			panic(err)
		}
	}

	if err := t.blobDir.Close(); err != nil {
		panic(err)
	}
}

func setUrlPrefix(urlstr string, prefix string) (string, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", eris.Wrap(err, "Invalid data path")
	}

	q := u.Query()
	q.Set("prefix", prefix)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func newTestLogCase(name string, baseDir string, c Config) *testLogCase {
	dataPathBase := baseDir
	blobURL, err := path.ConvertToBlobURL(baseDir)
	if err != nil {
		panic(err)
	}

	blobDir, err := util.NewBlobDir(blobURL)
	if err != nil {
		panic(err)
	}

	return &testLogCase{
		name:         name,
		blobDir:      blobDir,
		urlstr:       blobURL,
		config:       c,
		dataPathBase: dataPathBase,
	}
}

func newTestLogCases(names ...string) []*testLogCase {
	var cases []*testLogCase
	for _, name := range names {
		if name == "file" {
			cases = append(cases, newTestLogCase("file", getTestFileBaseDir(), getTestFileConfig()))
		} else if name == "azblob" {
			os.Setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
			os.Setenv("AZURE_STORAGE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
			os.Setenv("AZURE_STORAGE_DOMAIN", "localhost:10000")
			os.Setenv("AZURE_STORAGE_PROTOCOL", "http")
			os.Setenv("AZURE_STORAGE_IS_CDN", "false")
			os.Setenv("AZURE_STORAGE_IS_LOCAL_EMULATOR", "true")

			cases = append(cases, newTestLogCase("azblob", getTestAzBlobBaseDir(), getTestAzBlobConfig()))
		} else if name == "gs" {
			os.Setenv("STORAGE_EMULATOR_HOST", "localhost:4443")

			cases = append(cases, newTestLogCase("gs", getTestGoogleStorageBaseDir(), getTestGoogleStorageConfig()))
		} else {
			panic("unsupported test case for " + name)
		}
	}
	return cases
}

func TestLog_snapshot(t *testing.T) {
	t.Parallel()
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

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			table, err := tt.getLog("snapshot-data0")
			assert.NoError(t, err)
			s, err := table.Snapshot()
			assert.NoError(t, err)
			data0_files := getDirDataFiles(getTestFileDir("snapshot-data0"))
			verify(s, data0_files, 0)

			table, err = tt.getLog("snapshot-data1")
			assert.NoError(t, err)
			s, err = table.Snapshot()
			assert.NoError(t, err)
			data0_data1_files := getDirDataFiles(getTestFileDir("snapshot-data1"))
			verify(s, data0_data1_files, 1)

			table, err = tt.getLog("snapshot-data2")
			assert.NoError(t, err)
			s, err = table.Snapshot()
			assert.NoError(t, err)
			var data2_files []string
			data0_data1_fileset := mapset.NewSet(data0_data1_files...)
			// we have overwritten files for data0 & data1; only data2 files should remain
			for _, f := range getDirDataFiles(getTestFileDir("snapshot-data2")) {
				if !data0_data1_fileset.Contains(f) {
					data2_files = append(data2_files, f)
				}
			}
			verify(s, data2_files, 2)

			table, err = tt.getLog("snapshot-data3")
			assert.NoError(t, err)
			s, err = table.Snapshot()
			assert.NoError(t, err)
			var data2_data3_files []string
			for _, f := range getDirDataFiles(getTestFileDir("snapshot-data3")) {
				if !data0_data1_fileset.Contains(f) {
					data2_data3_files = append(data2_data3_files, f)
				}
			}
			verify(s, data2_data3_files, 3)

			table, err = tt.getLog("snapshot-data2-deleted")
			assert.NoError(t, err)
			s, err = table.Snapshot()
			assert.NoError(t, err)
			var data3_files []string
			data2_fileset := mapset.NewSet(data2_files...)
			for _, f := range getDirDataFiles(getTestFileDir("snapshot-data2-deleted")) {
				if !data0_data1_fileset.Contains(f) && !data2_fileset.Contains(f) {
					data3_files = append(data3_files, f)
				}
			}
			verify(s, data3_files, 4)

			table, err = tt.getLog("snapshot-repartitioned")
			assert.NoError(t, err)
			s, err = table.Snapshot()
			assert.NoError(t, err)
			allFiles, err := s.AllFiles()
			assert.NoError(t, err)
			assert.Equal(t, 2, len(allFiles))
			assert.Equal(t, int64(5), s.Version())

			table, err = tt.getLog("snapshot-vacuumed")
			assert.NoError(t, err)
			s, err = table.Snapshot()
			assert.NoError(t, err)
			verify(s, getDirDataFiles(getTestFileDir("snapshot-vacuumed")), 5)
		})
	}

}

func TestLog_checkpoint(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases(
		"file",
		"azblob",
		"gs",
	) {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			table, err := tt.getLog("checkpoint")
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
			var iterErr error
			var f *action.AddFile
			for f, iterErr = iter.Next(); iterErr == nil; f, iterErr = iter.Next() {
				res = append(res, f.Path)
			}
			assert.ErrorIs(t, iterErr, io.EOF)
			iter.Close()
			assert.Equal(t, "15", res[0])
		})
	}

}

func TestLog_updateDeletedDir(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {

		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			table, err := tt.copyLog("update-deleted-directory")
			assert.NoError(t, err)

			err = tt.blobDir.Delete(tt.copiedDir, tt.copiedFiles, true)
			assert.NoError(t, err)

			s, err := table.Update()
			assert.NoError(t, err)
			assert.Equal(t, int64(-1), s.Version())
		})
	}
}

func TestLog_update_should_not_pick_up_delta_files_earlier_than_checkpoint(t *testing.T) {
	t.Parallel()
	engineInfo := "test-engine-info"
	manualUpdate := &op.Operation{Name: op.MANUALUPDATE}
	metadata := getTestMetedata()

	for _, tt := range newTestLogCases(
		"file",
		"azblob", "gs",
	) {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			table1, err := tt.getTempLog()
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

			table2, err := ForTable(table1.Path(),
				getTestFileConfig(),
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
		})
	}
}

func TestLog_handle_corrupted_last_checkpoint_file(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			table, err := tt.copyLog("corrupted-last-checkpoint")
			assert.NoError(t, err)

			logImpl1 := table.(*logImpl)
			lc, err := LastCheckpoint(logImpl1.store)
			assert.NoError(t, err)
			assert.True(t, lc.IsPresent())

			lastcheckpoint1 := lc.MustGet()

			err = logImpl1.store.Create(logImpl1.logPath + LastCheckpointPath)
			assert.NoError(t, err)

			table2, err := ForTable(table.Path(),
				getTestFileConfig(),
				&SystemClock{})
			assert.NoError(t, err)

			lc2, err := LastCheckpoint(table2.(*logImpl).store)
			lastcheckpoint2 := lc2.MustGet()

			assert.NoError(t, err)
			assert.Equal(t, FromMetadata(*lastcheckpoint2), FromMetadata(*lastcheckpoint1))
		})
	}
}

func TestLog_paths_should_be_canonicalized_normal_characters(t *testing.T) {

	for _, tt := range newTestLogCases("file") {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.clean()

			log, err := tt.getLog("canonicalized-paths-normal-a")
			assert.NoError(t, err)

			s, err := log.Update()
			assert.NoError(t, err)
			assert.Equal(t, int64(1), s.Version())

			files, err := s.AllFiles()
			assert.NoError(t, err)
			assert.Equal(t, 0, len(files))

			log, err = tt.getLog("canonicalized-paths-normal-b")
			assert.NoError(t, err)

			s, err = log.Update()
			assert.NoError(t, err)
			assert.Equal(t, int64(1), s.Version())

			files, err = s.AllFiles()
			assert.NoError(t, err)
			assert.Equal(t, 0, len(files))
		})
	}
}

func TestLog_paths_should_be_canonicalized_special_characters(t *testing.T) {
	for _, tt := range newTestLogCases("file") {
		defer tt.clean()

		log, err := tt.getLog("canonicalized-paths-special-a")
		assert.NoError(t, err)

		s, err := log.Update()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), s.Version())

		files, err := s.AllFiles()
		assert.NoError(t, err)
		assert.Equal(t, 0, len(files))

		log, err = tt.getLog("canonicalized-paths-special-b")
		assert.NoError(t, err)

		s, err = log.Update()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), s.Version())

		files, err = s.AllFiles()
		assert.NoError(t, err)
		assert.Equal(t, 0, len(files))
	}
}

func TestLog_do_not_relative_path_in_remove_files(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			log, err := tt.getTempLog()
			assert.NoError(t, err)

			path := "schema://" + tt.tempDir + "/a/b/c"
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
			assert.Equal(t, path, commitedRemove[0].Path)
		})
	}

}

func TestLog_delete_and_readd_the_same_file_in_different_transactions(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			log, err := tt.getLog("delete-re-add-same-file-different-transactions")
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
		})
	}
}

func TestLog_version_not_continuous(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			_, err := tt.getLog("versions-not-contiguous")
			assert.ErrorIs(t, err, errno.DeltaVersionNotContinuous([]int64{0, 2}))
		})
	}
}

func TestLog_state_reconstruction_without_action_should_fail(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()
			for _, name := range []string{"protocol", "metadata"} {
				_, err := tt.getLog(fmt.Sprintf("deltalog-state-reconstruction-without-%s", name))
				assert.ErrorIs(t, err, errno.ActionNotFound(name, 0))
			}
		})
	}
}

func TestLog_state_reconstruction_from_checkpoint_with_missing_action_should_fail(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			for _, name := range []string{"protocol", "metadata"} {
				_, err := tt.getLog(fmt.Sprintf("deltalog-state-reconstruction-from-checkpoint-missing-%s", name))
				assert.ErrorIs(t, err, errno.ActionNotFound(name, 10))
			}
		})
	}
}

func TestLog_table_protocol_version_greater_than_client_reader_protocol_version(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			_, err := tt.getLog("deltalog-invalid-protocol-version")
			assert.ErrorIs(t, err, errno.InvalidProtocolVersionError())
		})
	}
}

func TestLog_get_commit_info(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			log, err := tt.getLog("deltalog-commit-info")
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
			log, err = tt.getLog("snapshot-vacuumed")
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
		})
	}
}

func TestLog_getChanges_no_data_loss(t *testing.T) {
	t.Parallel()
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

	for _, tt := range newTestLogCases(
		"file",
		"azblob",
		"gs",
	) {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			log, err := tt.getLog("deltalog-getChanges")
			assert.NoError(t, err)

			// standard cases
			verifyChanges(log, 0)
			verifyChanges(log, 1)
			verifyChanges(log, 2)

			// non-existant start version
			versionLogIter, err := log.Changes(3, false)
			assert.NoError(t, err)
			_, err = versionLogIter.Next()
			assert.ErrorIs(t, err, io.EOF)

			// negative start version
			_, err = log.Changes(-1, false)
			assert.ErrorIs(t, err, errno.ErrIllegalArgument)
		})
	}
}

func TestLog_getChanges_data_loss(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			log, err := tt.copyLog("deltalog-getChanges")
			assert.NoError(t, err)

			// delete 2 files
			err = tt.blobDir.DeleteFile(tt.copiedDir + "/_delta_log/00000000000000000000.json")
			assert.NoError(t, err)
			err = tt.blobDir.DeleteFile(tt.copiedDir + "/_delta_log/00000000000000000001.json")
			assert.NoError(t, err)

			vlIter, err := log.Changes(0, false)
			assert.NoError(t, err)
			versionLogs, err := iter.ToSlice(vlIter)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(versionLogs))

			_, err = log.Changes(0, true)
			assert.ErrorIs(t, err, errno.ErrIllegalState)
		})
	}
}

func TestLog_table_exists(t *testing.T) {
	t.Parallel()

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			log, err := tt.getTempLog()
			assert.NoError(t, err)
			assert.False(t, log.TableExists())

			trx, err := log.StartTransaction()
			assert.NoError(t, err)

			_, err = trx.Commit(iter.FromSlice([]action.Action{
				getTestMetedata(),
			}), &op.Operation{Name: op.CREATETABLE}, getTestEngineInfo())
			assert.NoError(t, err)

			assert.True(t, log.TableExists())
		})
	}

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

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			inputs := []tuple.T2[[]string, []string]{
				tuple.New2([]string{"a", "b"}, []string{}),
				tuple.New2([]string{}, []string{}),
				tuple.New2([]string{"a", "b", "c", "d"}, []string{"c", "d"}),
			}

			for _, i := range inputs {
				inputPartCols := i.V1
				missingPartCols := i.V2

				log, err := tt.getTempLog()
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
				// clean up temporary
				tt.cleanTempDir()
			}
		})
	}
}

func TestLog_schema_contains_no_data_columns_and_only_partition_columns(t *testing.T) {

	for _, tt := range newTestLogCases("file", "azblob", "gs") {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer tt.clean()

			log, err := tt.getTempLog()
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
		})
	}
}

// func TestLog_getVersionBeforeOrAtTimestamp_and_getVersionAtOrAfterTimestamp(t *testing.T) {
// 	t.Parallel()

// 	for _, tt := range newTestLogCases("file", "azblob", "gs") {
// 		tt := tt
// 		t.Run(tt.name, func(t *testing.T) {
// 			t.Parallel()
// 			defer tt.clean()

// 			log, err := tt.getTempLog()
// 			assert.NoError(t, err)

// 			// ========== case 0: delta table is empty ==========
// 			v, err := log.VersionBeforeOrAtTimestamp(time.Now().UnixMilli())
// 			assert.NoError(t, err)
// 			assert.Equal(t, int64(-1), v)
// 			v, err = log.VersionAtOrAfterTimestamp(time.Now().UnixMilli())
// 			assert.NoError(t, err)
// 			assert.Equal(t, int64(-1), v)

// 			for i := 0; i <= 2; i++ {
// 				trx, err := log.StartTransaction()
// 				assert.NoError(t, err)
// 				if i == 0 {
// 					assert.NoError(t, trx.UpdateMetadata(getTestMetedata()))
// 				}
// 				files := []action.Action{
// 					&action.AddFile{Path: strconv.Itoa(i), PartitionValues: map[string]string{}, Size: 1, ModificationTime: 1, DataChange: true},
// 				}
// 				_, err = trx.Commit(iter.FromSlice(files), getTestManualUpdate(), getTestEngineInfo())
// 				assert.NoError(t, err)
// 			}

// 			logPath := tt.tempDir + "/_delta_log/"
// 			delta0 := filenames.DeltaFile(logPath, 0)
// 			delta1 := filenames.DeltaFile(logPath, 1)
// 			delta2 := filenames.DeltaFile(logPath, 2)

// 			setLastModified := func(path string, ts int64) {
// 				os.Chtimes(path, time.Now(), time.UnixMilli(ts))
// 			}

// 			setLastModified(delta0, 1000)
// 			setLastModified(delta1, 2000)
// 			setLastModified(delta2, 3000)

// 			assertGetVersion := func(fn func(int64) (int64, error), input int64, expected int64) {
// 				actual, err := fn(input)
// 				assert.NoError(t, err)
// 				assert.Equal(t, expected, actual)
// 			}
// 			// ========== case 1: before first commit ==========
// 			_, err = log.VersionBeforeOrAtTimestamp(500)
// 			assert.ErrorIs(t, err, errno.ErrIllegalArgument)
// 			assertGetVersion(log.VersionAtOrAfterTimestamp, 500, 0)

// 			// ========== case 2: at first commit ==========
// 			assertGetVersion(log.VersionBeforeOrAtTimestamp, 1000, 0)
// 			assertGetVersion(log.VersionAtOrAfterTimestamp, 1000, 0)

// 			// ========== case 3: between two normal commits ==========
// 			assertGetVersion(log.VersionBeforeOrAtTimestamp, 1500, 0)
// 			assertGetVersion(log.VersionAtOrAfterTimestamp, 1500, 1)

// 			// // ========== case 4: at last commit ==========
// 			assertGetVersion(log.VersionBeforeOrAtTimestamp, 3000, 2)
// 			assertGetVersion(log.VersionAtOrAfterTimestamp, 3000, 2)

// 			// ========== case 5: after last commit ==========
// 			assertGetVersion(log.VersionBeforeOrAtTimestamp, 4000, 2)
// 			_, err = log.VersionAtOrAfterTimestamp(4000)
// 			assert.ErrorIs(t, err, errno.ErrIllegalArgument)
// 		})
// 	}

// }

func TestLog_getVersionBeforeOrAtTimestamp_and_getVersionAtOrAfterTimestamp_recoverability(t *testing.T) {

	cases := newTestLogCases("file")
	tt := cases[0]
	defer tt.clean()

	log, err := tt.getTempLog()
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

	u, err := url.Parse(tt.urlstr)
	assert.NoError(t, err)
	u.Scheme = ""
	logPath := u.Path + "/" + tt.tempDir + "/_delta_log/"
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
