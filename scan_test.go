package deltago

import (
	"io"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/op"
	"github.com/csimplestring/delta-go/types"
	"github.com/repeale/fp-go"
	"github.com/stretchr/testify/assert"
)

type scanTestFixture struct {
	op                   *op.Operation
	schema               *types.StructType
	schemaString         string
	partitionSchema      *types.StructType
	metadata             *action.Metadata
	files                []action.Action
	filesDataChangeFalse []action.Action
	metaConjunct         *types.EqualTo
	dataConjunct         *types.EqualTo
}

func mustNoError(err error) {
	if err != nil {
		panic(err)
	}
}

func (s *scanTestFixture) withLog(actions []action.Action) (Log, string) {
	dir, err := os.MkdirTemp("", "delta")
	mustNoError(err)

	log, err := ForTable("file://"+dir, getTestConfig(), &SystemClock{})
	mustNoError(err)

	trx, err := log.StartTransaction()
	mustNoError(err)
	_, err = trx.Commit(iter.FromSlice([]action.Action{s.metadata}), s.op, "engineInfo")
	mustNoError(err)

	trx, err = log.StartTransaction()
	mustNoError(err)
	_, err = trx.Commit(iter.FromSlice(actions), s.op, "engineInfo")
	mustNoError(err)

	return log, dir
}

func newScanTestFixtures() *scanTestFixture {
	op := &op.Operation{Name: op.WRITE}

	schema := types.NewStructType([]*types.StructField{
		types.NewStructField("col1", &types.IntegerType{}, true),
		types.NewStructField("col2", &types.IntegerType{}, true),
		types.NewStructField("col3", &types.IntegerType{}, true),
		types.NewStructField("col4", &types.IntegerType{}, true),
	})

	schemaString, err := types.ToJSON(schema)
	if err != nil {
		panic(err)
	}

	partitionSchema := types.NewStructType([]*types.StructField{
		types.NewStructField("col1", &types.IntegerType{}, true),
		types.NewStructField("col2", &types.IntegerType{}, true),
	})

	metadata := &action.Metadata{
		PartitionColumns: []string{"col1", "col2"},
		SchemaString:     schemaString,
	}

	files := make([]action.Action, 11)
	for i := 0; i <= 10; i++ {
		partitionValues := map[string]string{
			"col1": strconv.Itoa(i % 3),
			"col2": strconv.Itoa(i % 2),
		}
		files[i] = &action.AddFile{
			Path:             strconv.Itoa(i),
			PartitionValues:  partitionValues,
			Size:             1,
			ModificationTime: 1,
			DataChange:       true,
		}
	}

	filesDataChangeFalse := fp.Map(func(af action.Action) action.Action {
		a := af.(*action.AddFile)
		return a.Copy(false, a.Path)
	})(files)

	metadataConjunct := types.NewEqualTo(schema.Column("col1"), types.LiteralInt(0))
	dataConjunct := types.NewEqualTo(schema.Column("col3"), types.LiteralInt(5))

	return &scanTestFixture{
		op:                   op,
		schema:               schema,
		schemaString:         schemaString,
		partitionSchema:      partitionSchema,
		metadata:             metadata,
		files:                files,
		filesDataChangeFalse: filesDataChangeFalse,
		metaConjunct:         metadataConjunct,
		dataConjunct:         dataConjunct,
	}
}

func TestScan_properly_splits_metadata_pushed_and_data_residual_predicates(t *testing.T) {
	f := newScanTestFixtures()
	log, dir := f.withLog(f.files)
	defer os.RemoveAll(dir)

	mixedConjunct := types.NewLessThan(f.schema.Column("col2"), f.schema.Column("col4"))
	filter := types.NewAnd(types.NewAnd(f.metaConjunct, f.dataConjunct), mixedConjunct)

	s, err := log.Update()
	assert.NoError(t, err)

	scan, err := s.Scan(filter)
	assert.NoError(t, err)
	assert.Equal(t, f.metaConjunct.String(), scan.PushedPredicate().String())
	assert.Equal(t,
		types.NewAnd(f.dataConjunct, mixedConjunct).String(),
		scan.ResidualPredicate().String())
}

func TestScan_filtered_scan_with_a_metadata_pushed_conjunct_should_return_matched_files(t *testing.T) {
	f := newScanTestFixtures()
	log, dir := f.withLog(f.files)
	defer os.RemoveAll(dir)

	filter := types.NewAnd(f.metaConjunct, f.dataConjunct)
	s, err := log.Update()
	assert.NoError(t, err)

	scan, err := s.Scan(filter)
	assert.NoError(t, err)

	fIter, err := scan.Files()
	assert.NoError(t, err)

	addFiles, err := iter.ToSlice(fIter)
	assert.NoError(t, err)

	expected := fp.Filter(func(t *action.AddFile) bool {
		if v, ok := t.PartitionValues["col1"]; ok {
			return v == "0"
		}
		return false
	})(action.UtilFnCollect[*action.AddFile](f.filesDataChangeFalse))

	assert.Equal(t, expected, addFiles)
	assert.Equal(t, f.metaConjunct.String(), scan.PushedPredicate().String())
	assert.Equal(t, f.dataConjunct.String(), scan.ResidualPredicate().String())
}

func TestScan_filtered_scan_with_only_data_residual_predicate_should_return_all_files(t *testing.T) {
	f := newScanTestFixtures()
	log, dir := f.withLog(f.files)
	defer os.RemoveAll(dir)

	filter := f.dataConjunct
	s, err := log.Update()
	assert.NoError(t, err)

	scan, err := s.Scan(filter)
	assert.NoError(t, err)

	fIter, err := scan.Files()
	assert.NoError(t, err)

	addFiles, err := iter.ToSlice(fIter)
	assert.NoError(t, err)

	assert.Equal(t, action.UtilFnCollect[*action.AddFile](f.filesDataChangeFalse), addFiles)
	assert.Nil(t, scan.PushedPredicate())
	assert.Equal(t, filter.String(), scan.ResidualPredicate().String())
}

func TestScan_correct_reverse_replay(t *testing.T) {

	f := newScanTestFixtures()

	filter := types.NewAnd(
		types.NewEqualTo(f.partitionSchema.Column("col1"), types.LiteralInt(0)),
		types.NewEqualTo(f.partitionSchema.Column("col2"), types.LiteralInt(0)),
	)

	addA_1 := &action.AddFile{Path: "a", PartitionValues: map[string]string{"col1": "0", "col2": "0"}, Size: 1, ModificationTime: 10, DataChange: true}
	addA_2 := &action.AddFile{Path: "a", PartitionValues: map[string]string{"col1": "0", "col2": "0"}, Size: 1, ModificationTime: 20, DataChange: true}
	addB_4 := &action.AddFile{Path: "b", PartitionValues: map[string]string{"col1": "0", "col2": "1"}, Size: 1, ModificationTime: 40, DataChange: true}
	addC_7 := &action.AddFile{Path: "c", PartitionValues: map[string]string{"col1": "0", "col2": "0"}, Size: 1, ModificationTime: 70, DataChange: true}
	addD_8 := &action.AddFile{Path: "d", PartitionValues: map[string]string{"col1": "0", "col2": "0"}, Size: 1, ModificationTime: 80, DataChange: true}
	ts := int64(90)
	dc := true
	removeD_9 := addD_8.RemoveWithTimestamp(&ts, &dc)
	addE_13 := &action.AddFile{Path: "e", PartitionValues: map[string]string{"col1": "0", "col2": "0"}, Size: 1, ModificationTime: 10, DataChange: true}
	addF_16_0 := &action.AddFile{Path: "f", PartitionValues: map[string]string{"col1": "0", "col2": "0"}, Size: 1, ModificationTime: 130, DataChange: true}
	addF_16_1 := &action.AddFile{Path: "f", PartitionValues: map[string]string{"col1": "0", "col2": "0"}, Size: 1, ModificationTime: 131, DataChange: true}

	dir, err := os.MkdirTemp("", "delta")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	log, err := ForTable("file://"+dir, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	commit := func(actions []action.Action) {
		trx, err := log.StartTransaction()
		assert.NoError(t, err)
		_, err = trx.Commit(iter.FromSlice(actions), f.op, "engineInfo")
		assert.NoError(t, err)
	}

	commit([]action.Action{f.metadata})
	commit([]action.Action{addA_1})
	commit([]action.Action{addA_2})
	commit([]action.Action{})
	commit([]action.Action{addB_4})
	commit([]action.Action{})
	commit([]action.Action{})
	commit([]action.Action{addC_7})
	commit([]action.Action{addD_8})
	commit([]action.Action{removeD_9})
	commit([]action.Action{})
	commit([]action.Action{})
	commit([]action.Action{})
	commit([]action.Action{addE_13})
	commit([]action.Action{})
	commit([]action.Action{})
	commit([]action.Action{addF_16_0, addF_16_1})
	commit([]action.Action{})

	store := log.(*logImpl).store

	for _, i := range []int64{12, 14, 15, 17} {
		path := filenames.DeltaFile(store.Root(), i)
		err = store.Write(path, iter.FromSlice([]string{}), true)
		assert.NoError(t, err)
	}

	expectedSet := []*action.AddFile{
		addA_2.Copy(false, addA_2.Path),
		addC_7.Copy(false, addC_7.Path),
		addE_13.Copy(false, addE_13.Path),
		addF_16_0.Copy(false, addF_16_0.Path),
	}

	s, err := log.Update()
	assert.NoError(t, err)
	scan, err := s.Scan(filter)
	assert.NoError(t, err)
	iter, err := scan.Files()
	assert.NoError(t, err)
	defer iter.Close()

	var set []*action.AddFile
	var v *action.AddFile

	for v, err = iter.Next(); err == nil; v, err = iter.Next() {
		set = append(set, v)
	}
	assert.ErrorIs(t, err, io.EOF)

	// sort then compare
	sort.Slice(expectedSet, func(i, j int) bool {
		return expectedSet[i].Path < expectedSet[j].Path
	})
	sort.Slice(set, func(i, j int) bool {
		return set[i].Path < set[j].Path
	})
	assert.Equal(t, expectedSet, set)
}
