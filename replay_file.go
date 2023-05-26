package deltago

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/store"
	"github.com/rotisserie/eris"
	"github.com/samber/mo"
)

type replayTuple struct {
	act            action.Action
	fromCheckpoint bool
}

var _ iter.Iter[*replayTuple] = &customJSONIterator{}

type customJSONIterator struct {
	iter iter.Iter[string]
}

func (r *customJSONIterator) Next() (*replayTuple, error) {
	str, err := r.iter.Next()
	if err != nil {
		return nil, err
	}

	act := &action.SingleAction{}
	err = json.Unmarshal([]byte(str), &act)
	if err != nil {
		return nil, eris.Wrap(err, "")
	}

	return &replayTuple{
		act:            act.Unwrap(),
		fromCheckpoint: false,
	}, nil
}

func (r *customJSONIterator) Close() error {
	return r.iter.Close()
}

type customParquetIterator struct {
	iter iter.Iter[action.Action]
}

func (c *customParquetIterator) Next() (*replayTuple, error) {
	a, err := c.iter.Next()
	if err != nil {
		return nil, err
	}

	return &replayTuple{
		act:            a,
		fromCheckpoint: true,
	}, nil
}

func (c *customParquetIterator) Close() error {
	return c.iter.Close()
}

type MemoryOptimizedLogReplay struct {
	files    []string
	logStore store.Store
	//timezone      time.Location
	checkpointReader checkpointReader
}

func (m *MemoryOptimizedLogReplay) GetReverseIterator() iter.Iter[*replayTuple] {
	sort.Slice(m.files, func(i, j int) bool {
		return m.files[i] > m.files[j]
	})
	reverseFilesIter := iter.FromSlice(m.files)

	return &logReplayIterator{
		logStore:         m.logStore,
		checkpointReader: m.checkpointReader,
		reverseFilesIter: reverseFilesIter,
		actionIter:       mo.None[iter.Iter[*replayTuple]](),
	}
}

type logReplayIterator struct {
	logStore         store.Store
	checkpointReader checkpointReader
	reverseFilesIter iter.Iter[string]
	actionIter       mo.Option[iter.Iter[*replayTuple]]
}

func (l *logReplayIterator) Next() (*replayTuple, error) {
	if l.actionIter.IsAbsent() {
		it, err := l.getNextIter()
		if err != nil {
			return nil, err
		}
		l.actionIter = mo.Some(it)
	}

	r, err := l.actionIter.MustGet().Next()
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		// reach end of file, close
		if err := l.actionIter.MustGet().Close(); err != nil {
			return nil, err
		}
		// continue to call next file
		l.actionIter = mo.None[iter.Iter[*replayTuple]]()
		return l.Next()
	}

	return r, nil
}

func (l *logReplayIterator) getNextIter() (iter.Iter[*replayTuple], error) {

	nextFile, err := l.reverseFilesIter.Next()
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(nextFile, ".json") {
		iter, err := l.logStore.Read(nextFile)
		return &customJSONIterator{iter: iter}, err
	} else if strings.HasSuffix(nextFile, ".parquet") {
		iter, err := l.checkpointReader.Read(nextFile)
		return &customParquetIterator{iter: iter}, err
	} else {
		return nil, fmt.Errorf("unexpected log file path: %s", nextFile)
	}
}

// func (l *logReplayIterator) ensureNextIterReady() error {
// 	if l.actionIter.IsPresent() && l.actionIter.MustGet().Next() {
// 		return nil
// 	}

// 	if l.actionIter.IsPresent() {
// 		if err := l.actionIter.MustGet().Close(); err != nil {
// 			return err
// 		}
// 	}

// 	l.actionIter = mo.None[iter.Iter[*replayTuple]]()

// 	for l.reverseFilesIter.Next() {
// 		fiter, err := l.getNextIter()
// 		if err != nil {
// 			return err
// 		}
// 		l.actionIter = mo.Some(fiter)

// 		if l.actionIter.MustGet().Next() {
// 			return nil
// 		}

// 		if err := l.actionIter.MustGet().Close(); err != nil {
// 			return err
// 		}

// 		l.actionIter = mo.None[iter.Iter[*replayTuple]]()
// 	}

// 	return nil
// }

// func (l *logReplayIterator) Next() bool {
// 	if err := l.ensureNextIterReady(); err != nil {
// 		return false
// 	}

// 	return l.actionIter.IsPresent()
// }

// func (l *logReplayIterator) Value() (*replayTuple, error) {
// 	if !l.Next() {
// 		return nil, eris.New("no element")
// 	}
// 	if l.actionIter.IsAbsent() {
// 		return nil, eris.New("impossible")
// 	}
// 	return l.actionIter.MustGet().Value()
// }

func (l *logReplayIterator) Close() error {
	if l.actionIter.IsPresent() {
		return l.actionIter.MustGet().Close()
	}
	return nil
}
