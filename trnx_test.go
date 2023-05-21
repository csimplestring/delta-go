package deltago

import (
	"os"
	"testing"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/op"
	"github.com/csimplestring/delta-go/types"
	"github.com/stretchr/testify/assert"
)

type trxTestFixture struct {
	addA           *action.AddFile
	addB           *action.AddFile
	removeA        *action.RemoveFile
	removeA_time5  *action.RemoveFile
	addA_partX1    *action.AddFile
	addA_partX2    *action.AddFile
	addB_partX1    *action.AddFile
	addB_partX3    *action.AddFile
	addC_partX4    *action.AddFile
	schema         *types.StructType
	colXEq1Filter  *types.EqualTo
	metadata_colXY *action.Metadata
	metadata_partX *action.Metadata
	op             *op.Operation
	engineInfo     string
}

func newTrxTestFixture() *trxTestFixture {

	addA := &action.AddFile{Path: "a", PartitionValues: map[string]string{}, Size: 1, ModificationTime: 1, DataChange: true}

	addB := &action.AddFile{Path: "b", PartitionValues: map[string]string{}, Size: 1, ModificationTime: 1, DataChange: true}

	removeA := addA.RemoveWithTimestamp(util.PtrOf[int64](4), &addA.DataChange)
	removeA_time5 := addA.RemoveWithTimestamp(util.PtrOf[int64](5), &addA.DataChange)

	addA_partX1 := &action.AddFile{Path: "a", PartitionValues: map[string]string{"x": "1"}, Size: 1, ModificationTime: 1, DataChange: true}
	addA_partX2 := &action.AddFile{Path: "a", PartitionValues: map[string]string{"x": "2"}, Size: 1, ModificationTime: 1, DataChange: true}
	addB_partX1 := &action.AddFile{Path: "b", PartitionValues: map[string]string{"x": "1"}, Size: 1, ModificationTime: 1, DataChange: true}
	addB_partX3 := &action.AddFile{Path: "b", PartitionValues: map[string]string{"x": "3"}, Size: 1, ModificationTime: 1, DataChange: true}
	addC_partX4 := &action.AddFile{Path: "c", PartitionValues: map[string]string{"x": "4"}, Size: 1, ModificationTime: 1, DataChange: true}

	schema := types.NewStructType([]*types.StructField{
		types.NewStructField("x", &types.IntegerType{}, true),
		types.NewStructField("y", &types.IntegerType{}, true),
	})
	schemaString, err := types.ToJSON(schema)
	if err != nil {
		panic(err)
	}

	colXEq1Filter := types.NewEqualTo(schema.Column("x"), types.LiteralInt(1))
	metadata_colXY := action.Metadata{SchemaString: schemaString}
	metadata_partX := action.Metadata{SchemaString: schemaString, PartitionColumns: []string{"x"}}

	return &trxTestFixture{
		addA:           addA,
		addB:           addB,
		removeA:        removeA,
		removeA_time5:  removeA_time5,
		addA_partX1:    addA_partX1,
		addA_partX2:    addA_partX2,
		addB_partX1:    addB_partX1,
		addB_partX3:    addB_partX3,
		addC_partX4:    addC_partX4,
		schema:         schema,
		colXEq1Filter:  colXEq1Filter,
		metadata_colXY: &metadata_colXY,
		metadata_partX: &metadata_partX,
		op:             &op.Operation{Name: op.MANUALUPDATE},
		engineInfo:     "test-engine-info",
	}
}

func setUpTestTrxLog(setup []action.Action, log Log, f *trxTestFixture, t *testing.T) {

	for _, s := range setup {
		trx, err := log.StartTransaction()
		assert.NoError(t, err)

		_, err = trx.Commit(iter.FromSlice([]action.Action{s}), f.op, f.engineInfo)
		assert.NoError(t, err)
	}
}

func getTempLog(t *testing.T) (Log, string) {
	dir, err := os.MkdirTemp("", "delta")
	assert.NoError(t, err)

	log, err := ForTable("file://"+dir, getTestConfig(), &SystemClock{})
	assert.NoError(t, err)

	return log, dir
}

func checkTrx(
	conflict bool,
	log Log,
	reads []func(OptimisticTransaction),
	concurrentWrites []func(OptimisticTransaction, []action.Action),
	concurrentWritesActions []action.Action,
	actions []action.Action,
	op *op.Operation,
	engineInfo string,
	t *testing.T) {

	trx, err := log.StartTransaction()
	assert.NoError(t, err)
	for _, r := range reads {
		r(trx)
	}

	for _, w := range concurrentWrites {
		trx2, err := log.StartTransaction()
		assert.NoError(t, err)
		w(trx2, concurrentWritesActions)
	}

	if conflict {
		_, err = trx.Commit(iter.FromSlice(actions), op, engineInfo)
		assert.ErrorAs(t, err, &errno.ErrConcurrentModification)
	} else {
		_, err = trx.Commit(iter.FromSlice(actions), op, engineInfo)
		assert.NoError(t, err)
	}

}

func TestTrx_apend_apend(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_colXY, &action.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(trx OptimisticTransaction) {
			_, err := trx.Metadata()
			assert.NoError(t, err)
		},
	}
	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}
	conflict := false
	checkTrx(conflict, log, reads, concurrentWrites, []action.Action{f.addA}, []action.Action{f.addB}, f.op, f.engineInfo, t)
}

func TestTrx_disjoint_txns(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_colXY, &action.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(trx OptimisticTransaction) {
			_, err := trx.TxnVersion("t1")
			assert.NoError(t, err)
		},
	}
	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}
	concurrentWritesActions := []action.Action{&action.SetTransaction{AppId: "t2", Version: 0, LastUpdated: util.PtrOf[int64](1234)}}
	actions := []action.Action{}
	conflict := false
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_disjoint_delete_reads(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_partX, f.addA_partX2}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(trx OptimisticTransaction) {
			_, err := trx.MarkFilesAsRead(f.colXEq1Filter)
			assert.NoError(t, err)
		},
	}
	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}
	concurrentWritesActions := []action.Action{f.removeA}
	actions := []action.Action{}
	conflict := false
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_disjoint_add_reads(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_partX}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(trx OptimisticTransaction) {
			_, err := trx.MarkFilesAsRead(f.colXEq1Filter)
			assert.NoError(t, err)
		},
	}
	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}
	concurrentWritesActions := []action.Action{f.addA_partX2}
	actions := []action.Action{}
	conflict := false
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_add_rea_no_write(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_partX}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(trx OptimisticTransaction) {
			_, err := trx.MarkFilesAsRead(f.colXEq1Filter)
			assert.NoError(t, err)
		},
	}
	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}
	concurrentWritesActions := []action.Action{f.addA_partX1}
	actions := []action.Action{}
	conflict := false
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_delete_delete(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_colXY, &action.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){}

	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}
	concurrentWritesActions := []action.Action{f.removeA}
	actions := []action.Action{f.removeA_time5}
	conflict := true
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_add_read_write(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_partX}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(ot OptimisticTransaction) {
			_, err := ot.MarkFilesAsRead(f.colXEq1Filter)
			assert.NoError(t, err)
		},
	}

	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}
	concurrentWritesActions := []action.Action{f.addA_partX1}
	actions := []action.Action{f.addB_partX1}
	conflict := true
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_delete_read(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_partX, f.addA_partX1}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(ot OptimisticTransaction) {
			_, err := ot.MarkFilesAsRead(f.colXEq1Filter)
			assert.NoError(t, err)
		},
	}

	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}
	concurrentWritesActions := []action.Action{f.removeA}
	actions := []action.Action{}
	conflict := true
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_schema_change(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_colXY, &action.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(ot OptimisticTransaction) {
			_, err := ot.Metadata()
			assert.NoError(t, err)
		},
	}

	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}

	schemaString, err := types.ToJSON(types.NewStructType([]*types.StructField{
		types.NewStructField("foo", &types.IntegerType{}, true),
	}))
	assert.NoError(t, err)

	concurrentWritesActions := []action.Action{&action.Metadata{SchemaString: schemaString}}
	actions := []action.Action{}
	conflict := true
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_conflicting_txns(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_colXY, &action.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(ot OptimisticTransaction) {
			_, err := ot.TxnVersion("t1")
			assert.NoError(t, err)
		},
	}

	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}

	concurrentWritesActions := []action.Action{&action.SetTransaction{AppId: "t1", LastUpdated: util.PtrOf[int64](1234)}}
	actions := []action.Action{}
	conflict := true
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_upgrade_upgrade(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_colXY, &action.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(ot OptimisticTransaction) {
			_, err := ot.Metadata()
			assert.NoError(t, err)
		},
	}

	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}

	concurrentWritesActions := []action.Action{&action.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}}
	actions := []action.Action{&action.Protocol{MinReaderVersion: 1, MinWriterVersion: 2}}
	conflict := true
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_taint_whole_table(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_partX, f.addA_partX2}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(ot OptimisticTransaction) {
			_, err := ot.MarkFilesAsRead(f.colXEq1Filter)
			assert.NoError(t, err)
		},
		func(ot OptimisticTransaction) {
			err := ot.ReadWholeTable()
			assert.NoError(t, err)
		},
	}

	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}

	concurrentWritesActions := []action.Action{f.addB_partX3}
	actions := []action.Action{f.addC_partX4}
	conflict := true
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_taint_whole_table_concurrent_remove(t *testing.T) {
	f := newTrxTestFixture()
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	// setup
	setUpTestTrxLog([]action.Action{f.metadata_colXY, f.addA}, log, f, t)

	// reads
	reads := []func(OptimisticTransaction){
		func(ot OptimisticTransaction) {
			err := ot.ReadWholeTable()
			assert.NoError(t, err)
		},
	}

	// concurrentWrites
	concurrentWrites := []func(OptimisticTransaction, []action.Action){
		func(trx OptimisticTransaction, writes []action.Action) {
			_, err := trx.Commit(iter.FromSlice(writes), f.op, f.engineInfo)
			assert.NoError(t, err)
		},
	}

	concurrentWritesActions := []action.Action{f.removeA}
	actions := []action.Action{f.addB}
	conflict := true
	checkTrx(conflict, log, reads, concurrentWrites, concurrentWritesActions, actions, f.op, f.engineInfo, t)
}

func TestTrx_can_change_schema_to_valid_schema(t *testing.T) {

}

func TestTrx_converts_absolute_path_to_relative_path_when_in_table_path(t *testing.T) {
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	f := newTrxTestFixture()
	txn, err := log.StartTransaction()
	assert.NoError(t, err)

	err = txn.UpdateMetadata(f.metadata_colXY)
	assert.NoError(t, err)

	addFile := &action.AddFile{Path: dir + "/_delta_log/path/to/file/test.parquet", PartitionValues: map[string]string{}, DataChange: true}
	_, err = txn.Commit(iter.FromSlice([]action.Action{addFile}), f.op, "test")
	assert.NoError(t, err)

	s, err := log.Update()
	assert.NoError(t, err)

	allFiels, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, "path/to/file/test.parquet", allFiels[0].Path)
}

func TestTrx_relative_path_is_unchanged(t *testing.T) {
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	f := newTrxTestFixture()
	txn, err := log.StartTransaction()
	assert.NoError(t, err)

	err = txn.UpdateMetadata(f.metadata_colXY)
	assert.NoError(t, err)

	addFile := &action.AddFile{Path: "path/to/file/test.parquet", PartitionValues: map[string]string{}, DataChange: true}
	_, err = txn.Commit(iter.FromSlice([]action.Action{addFile}), f.op, "test")
	assert.NoError(t, err)

	s, err := log.Update()
	assert.NoError(t, err)

	allFiels, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, "path/to/file/test.parquet", allFiels[0].Path)
}

func TestTrx_absolute_path_is_unaltered_and_made_fully_qualified_when_not_in_table_path(t *testing.T) {
	log, dir := getTempLog(t)
	defer os.RemoveAll(dir)

	f := newTrxTestFixture()
	txn, err := log.StartTransaction()
	assert.NoError(t, err)

	err = txn.UpdateMetadata(f.metadata_colXY)
	assert.NoError(t, err)

	addFile := &action.AddFile{Path: "/absolute/path/to/file/test.parquet", PartitionValues: map[string]string{}, DataChange: true}
	_, err = txn.Commit(iter.FromSlice([]action.Action{addFile}), f.op, "test")
	assert.NoError(t, err)

	s, err := log.Update()
	assert.NoError(t, err)

	allFiels, err := s.AllFiles()
	assert.NoError(t, err)
	assert.Equal(t, "file:///absolute/path/to/file/test.parquet", allFiels[0].Path)
}
