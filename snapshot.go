package deltago

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/barweiss/go-tuple"
	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/store"
	expr "github.com/csimplestring/delta-go/types"
	"github.com/rotisserie/eris"
)

// Snapshot provides APIs to access the Delta table state (such as table metadata, active files) at some version.
// See Delta Transaction Log Protocol  for more details about the transaction logs.
type Snapshot interface {

	// Scan scan of the files in this snapshot matching the pushed portion of predicate
	Scan(predicate expr.Expression) (Scan, error)

	// AllFiles returns all of the files present in this snapshot
	AllFiles() ([]*action.AddFile, error)

	// Metadata returns the table metadata for this snapshot
	Metadata() (*action.Metadata, error)

	// Version returns the versiion of this Snapshot
	Version() int64

	// todo: i do not want to implement this for now
	// CloseableIterator<RowRecord> open();
}

type snapshotState struct {
	setTransactions      []*action.SetTransaction
	activeFiles          iter.Iter[*action.AddFile]
	tombstones           iter.Iter[*action.RemoveFile]
	sizeInBytes          int64
	numOfFiles           int64
	numOfRemoves         int64
	numOfSetTransactions int64
}

type snapshotImp struct {
	config                    Config
	path                      string
	version                   int64
	logSegment                *LogSegment
	minFileRetentionTimestamp int64
	timestamp                 int64
	store                     store.Store
	checkpointReader          checkpointReader

	state               *util.Lazy[*snapshotState]
	activeFiles         *util.Lazy[[]*action.AddFile]
	protocolAndMetadata *util.Lazy[*tuple.T2[*action.Protocol, *action.Metadata]]

	memoryOptimizedLogReplay *MemoryOptimizedLogReplay
}

func newSnapshotImp(config Config, path string, version int64, logsegment *LogSegment,
	minFileRetentionTimestamp int64, timestamp int64, store store.Store, checkpointReader checkpointReader) (*snapshotImp, error) {
	s := &snapshotImp{
		config:                    config,
		path:                      path,
		version:                   version,
		logSegment:                logsegment,
		minFileRetentionTimestamp: minFileRetentionTimestamp,
		timestamp:                 timestamp,
		store:                     store,
		checkpointReader:          checkpointReader,
	}

	s.memoryOptimizedLogReplay = &MemoryOptimizedLogReplay{
		files:            s.files(),
		logStore:         s.store,
		checkpointReader: s.checkpointReader,
	}

	s.state = util.LazyValue(s.loadState)
	s.activeFiles = util.LazyValue(s.loadActiveFiles)
	s.protocolAndMetadata = util.LazyValue(s.loadTableProtoclAndMetadata)

	t, err := s.protocolAndMetadata.Get()
	if err != nil {
		return nil, eris.Wrap(err, "fail to get protocol and metadata when initializing snapshots")
	}

	return s, assertProtocolRead(t.V1)
}

// Scan scan of the files in this snapshot matching the pushed portion of predicate
func (s *snapshotImp) Scan(predicate expr.Expression) (Scan, error) {
	if predicate == nil {
		return &scan{
			replay:       s.memoryOptimizedLogReplay,
			fileAccepter: &dummyAccepter{result: true},
			config:       s.config,
		}, nil
	}

	metadata, err := s.Metadata()
	if err != nil {
		return nil, err
	}

	ps, err := metadata.PartitionSchema()
	if err != nil {
		return nil, err
	}

	return newFilteredScan(s.memoryOptimizedLogReplay, s.config, predicate, ps)
}

// AllFiles returns all of the files present in this snapshot
func (s *snapshotImp) AllFiles() ([]*action.AddFile, error) {
	return s.activeFiles.Get()
}

// Metadata returns the table metadata for this snapshot
func (s *snapshotImp) Metadata() (*action.Metadata, error) {
	t, err := s.protocolAndMetadata.Get()
	return t.V2, err
}

// Version returns the versiion of this Snapshot
func (s *snapshotImp) Version() int64 {
	return s.version
}

func (s *snapshotImp) tombstones() ([]*action.RemoveFile, error) {
	state, err := s.state.Get()
	if err != nil {
		return nil, err
	}
	return iter.ToSlice(state.tombstones)
}

func (s *snapshotImp) setTransactions() []*action.SetTransaction {
	state, err := s.state.Get()
	if err != nil {
		return nil
	}
	return state.setTransactions
}

func (s *snapshotImp) transactions() map[string]int64 {
	// appID to version
	trxs := s.setTransactions()
	res := make(map[string]int64, len(trxs))
	for _, trx := range trxs {
		res[trx.AppId] = int64(trx.Version)
	}
	return res
}

func (s *snapshotImp) numOfFiles() (int64, error) {
	state, err := s.state.Get()
	if err != nil {
		return -1, err
	}
	return state.numOfFiles, nil
}

func (s *snapshotImp) files() []string {
	var res []string
	for _, f := range s.logSegment.Deltas {
		res = append(res, f.Path())
	}
	for _, f := range s.logSegment.Checkpoints {
		res = append(res, f.Path())
	}
	// todo: assert
	return res
}

func (s *snapshotImp) loadTableProtoclAndMetadata() (*tuple.T2[*action.Protocol, *action.Metadata], error) {
	var protocol *action.Protocol = nil
	var metadata *action.Metadata = nil
	iter := s.memoryOptimizedLogReplay.GetReverseIterator()
	defer iter.Close()

	for iter.Next() {
		replayTuple, err := iter.Value()
		if err != nil {
			return nil, err
		}
		a := replayTuple.act
		switch v := a.(type) {
		case *action.Protocol:
			if protocol == nil {
				protocol = v
				if protocol != nil && metadata != nil {
					res := tuple.New2(protocol, metadata)
					return &res, nil
				}
			}
		case *action.Metadata:
			if metadata == nil {
				metadata = v
				if metadata != nil && protocol != nil {
					res := tuple.New2(protocol, metadata)
					return &res, nil
				}
			}
		}
	}

	if protocol == nil {
		return nil, errno.ActionNotFound("protocol", s.logSegment.Version)
	}
	if metadata == nil {
		return nil, errno.ActionNotFound("metadata", s.logSegment.Version)
	}
	return nil, eris.Wrap(errno.ErrIllegalState, "should not happen")
}

func (s *snapshotImp) loadInMemory(files []string) ([]*action.SingleAction, error) {
	sort.Slice(files, func(i, j int) bool {
		return files[i] < files[j]
	})

	var actions []*action.SingleAction
	for _, f := range files {
		if strings.HasSuffix(f, "json") {
			iter, err := s.store.Read(f)
			if err != nil {
				return nil, err
			}

			for iter.Next() {
				s, err := iter.Value()
				if err != nil {
					return nil, err
				}
				action := &action.SingleAction{}
				if err := json.Unmarshal([]byte(s), &action); err != nil {
					return nil, eris.Wrap(err, "")
				}
				actions = append(actions, action)
			}
			iter.Close()
		} else if strings.HasSuffix(f, "parquet") {
			iter, err := s.checkpointReader.Read(f)
			if err != nil {
				return nil, err
			}
			for iter.Next() {
				s, err := iter.Value()
				if err != nil {
					return nil, err
				}

				actions = append(actions, s.Wrap())
			}
			iter.Close()
		}
	}
	return actions, nil
}

func (s *snapshotImp) loadState() (*snapshotState, error) {
	replay := NewInMemoryLogReplayer(s.minFileRetentionTimestamp, s.config.StorageConfig)
	singleActions, err := s.loadInMemory(s.files())
	if err != nil {
		return nil, err
	}

	actions := make([]action.Action, len(singleActions))
	for i, sa := range singleActions {
		actions[i] = sa.Unwrap()
	}
	replay.Append(0, iter.FromSlice(actions))

	if replay.currentProtocolVersion == nil {
		return nil, errno.ActionNotFound("protocl", s.version)
	}
	if replay.currentMetaData == nil {
		return nil, errno.ActionNotFound("metadata", s.version)
	}

	return &snapshotState{
		setTransactions:      replay.GetSetTransactions(),
		activeFiles:          replay.GetActiveFiles(),
		tombstones:           replay.GetTombstones(),
		sizeInBytes:          replay.sizeInBytes,
		numOfFiles:           int64(len(replay.activeFiles)),
		numOfRemoves:         int64(len(replay.tombstones)),
		numOfSetTransactions: int64(len(replay.transactions)),
	}, nil
}

func (s *snapshotImp) loadActiveFiles() ([]*action.AddFile, error) {
	v, err := s.state.Get()
	if err != nil {
		return nil, err
	}
	return iter.ToSlice(v.activeFiles)
}

func newInitialSnapshotImp(config Config, path string, store store.Store, cpReader checkpointReader) (*snapshotImp, error) {

	s := &snapshotImp{
		config:                    config,
		path:                      path,
		version:                   -1,
		logSegment:                emptyLogSegment(path),
		minFileRetentionTimestamp: -1,
		timestamp:                 -1,
		store:                     store,
		checkpointReader:          cpReader,
	}

	s.activeFiles = util.LazyValue(s.loadActiveFiles)

	// override the default
	s.memoryOptimizedLogReplay = &MemoryOptimizedLogReplay{
		files:            nil,
		logStore:         store,
		checkpointReader: cpReader,
	}
	s.state = util.LazyValue(func() (*snapshotState, error) {
		return &snapshotState{}, nil
	})
	s.protocolAndMetadata = util.LazyValue(func() (*tuple.T2[*action.Protocol, *action.Metadata], error) {
		t := tuple.New2(action.DefaultProtocol(), action.DefaultMetadata())
		return &t, nil
	})

	return s, nil
}
