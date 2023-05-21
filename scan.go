package deltago

import (
	"io"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/internal/util/path"
	"github.com/csimplestring/delta-go/iter"
	expr "github.com/csimplestring/delta-go/types"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/rotisserie/eris"
	"github.com/samber/mo"
)

// Scan provides access to an iterator over the files in this snapshot.
// Typically created with a read predicate Expression to let users filter files.
// Please note filtering is only supported on partition columns and
// users should use getResidualPredicate() to check for any unapplied portion of the input predicate.
type Scan interface {

	// Creates a CloseableIterator over files belonging to this snapshot.
	// There is no iteration ordering guarantee among files.
	// Files returned are guaranteed to satisfy the predicate, if any, returned by getPushedPredicate().
	// Returns:
	// a CloseableIterator over the files in this snapshot that satisfy getPushedPredicate()
	Files() (iter.Iter[*action.AddFile], error)

	// InputPredicate Returns the input predicate passed in by the user
	InputPredicate() expr.Expression

	// PushedPredicate Returns portion of the input predicate that can be evaluated by Delta Standalone
	// using only metadata (filters on partition columns).
	// Files returned by Files() are guaranteed to satisfy the pushed predicate, and the caller doesn’t need to apply them again on the returned files.
	PushedPredicate() expr.Expression

	// ResidualPredicate Returns portion of the input predicate that may not be fully applied.
	// Files returned by Files() are not guaranteed to satisfy the residual predicate, and the caller should still apply them on the returned files.
	ResidualPredicate() expr.Expression
}

type accepter interface {
	accept(addFile *action.AddFile) bool
}

type dummyAccepter struct {
	result bool
}

func (d *dummyAccepter) accept(addFile *action.AddFile) bool {
	return d.result
}

type scan struct {
	replay       *MemoryOptimizedLogReplay
	fileAccepter accepter
	config       Config
}

func (s *scan) Files() (iter.Iter[*action.AddFile], error) {
	return &scanFileIterator{
		iter:         s.replay.GetReverseIterator(),
		addFiles:     mapset.NewSet[string](),
		tombstones:   mapset.NewSet[string](),
		nextMatching: mo.None[*action.AddFile](),
		config:       s.config,
		fileAccepter: s.fileAccepter,
	}, nil
}

func (s *scan) InputPredicate() expr.Expression {
	return nil
}

func (s *scan) PushedPredicate() expr.Expression {
	return nil
}

func (s *scan) ResidualPredicate() expr.Expression {
	return nil
}

type scanFileIterator struct {
	iter         iter.Iter[*replayTuple]
	addFiles     mapset.Set[string]
	tombstones   mapset.Set[string]
	nextMatching mo.Option[*action.AddFile]
	config       Config
	fileAccepter accepter
}

func (s *scanFileIterator) findNextValid() (mo.Option[*action.AddFile], error) {
	var err error
	for tuple, err := s.iter.Next(); err == nil; tuple, err = s.iter.Next() {

		isCheckpoint := tuple.fromCheckpoint

		switch a := tuple.act.(type) {
		case *action.AddFile:
			canonicalPath, err := path.Canonicalize(a.Path, s.config.StoreType)
			if err != nil {
				return mo.None[*action.AddFile](), err
			}

			canonicalizeAdd := a.Copy(false, canonicalPath)
			alreadyDeleted := s.tombstones.Contains(canonicalizeAdd.Path)
			alreadyReturned := s.addFiles.Contains(canonicalizeAdd.Path)

			if !alreadyReturned {
				if !isCheckpoint {
					s.addFiles.Add(canonicalizeAdd.Path)
				}
				if !alreadyDeleted {
					return mo.Some(canonicalizeAdd), nil
				}
			}

		case *action.RemoveFile:
			if !isCheckpoint {
				canonicalPath, err := path.Canonicalize(a.Path, s.config.StoreType)
				if err != nil {
					return mo.None[*action.AddFile](), err
				}
				canonicalizeRemove := a.Copy(false, canonicalPath)
				s.tombstones.Add(canonicalizeRemove.Path)
			}
		}
	}
	if err != nil && err != io.EOF {
		return mo.None[*action.AddFile](), eris.Wrap(err, "")
	}
	return mo.None[*action.AddFile](), nil
}

func (s *scanFileIterator) setNextMatching() error {
	nextValid, err := s.findNextValid()
	if err != nil {
		return eris.Wrap(err, "")
	}

	for nextValid.IsPresent() {
		if s.fileAccepter.accept(nextValid.MustGet()) {
			s.nextMatching = nextValid
			return nil
		}

		nextValid, err = s.findNextValid()
		if err != nil {
			return eris.Wrap(err, "")
		}
	}

	s.nextMatching = mo.None[*action.AddFile]()
	return nil
}

func (s *scanFileIterator) hasNext() bool {
	if s.nextMatching.IsAbsent() {
		s.setNextMatching()
	}
	return s.nextMatching.IsPresent()
}

func (s *scanFileIterator) Next() (*action.AddFile, error) {
	if !s.hasNext() {
		return nil, io.EOF
	}
	// val ret = nextMatching.get
	// nextMatching = None
	// ret
	ret := s.nextMatching.MustGet()
	s.nextMatching = mo.None[*action.AddFile]()
	return ret, nil
}

func (s *scanFileIterator) Close() error {
	return s.iter.Close()
}

type filteredScanAccepter struct {
	metadataConjunction mo.Option[expr.Expression]
	partitionSchema     *expr.StructType
}

func (f *filteredScanAccepter) accept(addFile *action.AddFile) bool {
	if f.metadataConjunction.IsAbsent() {
		return true
	}

	r := &PartitionRowRecord{
		partitionSchema: f.partitionSchema,
		partitionValues: addFile.PartitionValues,
	}
	result, err := f.metadataConjunction.MustGet().Eval(r)
	if err != nil {
		panic(err) // TODO do not panic here
	}
	return result.(bool)
}

type filteredScan struct {
	s                   *scan
	exp                 expr.Expression
	metadataConjunction expr.Expression
	dataConjunction     expr.Expression
	partitionSchema     *expr.StructType
}

func newFilteredScan(replay *MemoryOptimizedLogReplay, config Config, exp expr.Expression, partitionSchema *expr.StructType) (*filteredScan, error) {

	// extract
	metadataConjunction, dataConjunction := util.SplitMetadataAndDataPredicates(exp, partitionSchema.FieldNames())

	s := &scan{
		replay: replay,
		fileAccepter: &filteredScanAccepter{
			metadataConjunction: metadataConjunction,
			partitionSchema:     partitionSchema,
		},
		config: config,
	}
	f := &filteredScan{
		s:                   s,
		exp:                 exp,
		metadataConjunction: metadataConjunction.OrEmpty(),
		dataConjunction:     dataConjunction.OrEmpty(),
		partitionSchema:     partitionSchema,
	}

	return f, nil
}

func (f *filteredScan) Files() (iter.Iter[*action.AddFile], error) {
	return f.s.Files()
}

func (f *filteredScan) InputPredicate() expr.Expression {
	return f.exp
}

func (f *filteredScan) PushedPredicate() expr.Expression {
	return f.metadataConjunction
}

func (f *filteredScan) ResidualPredicate() expr.Expression {
	return f.dataConjunction
}
