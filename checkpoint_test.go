package deltago_test

import (
	"fmt"
	"io"
	"testing"

	delta "github.com/csimplestring/delta-go"
	"github.com/rotisserie/eris"
	"github.com/samber/mo"

	"github.com/csimplestring/delta-go/iter"
	"github.com/csimplestring/delta-go/store"
	"github.com/stretchr/testify/assert"
)

func TestFindLastCompleteCheckpoint(t *testing.T) {

	s, err := newMemLogStore()
	assert.NoError(t, err)

	t.Run("returns err when first call to iterator fails with an error different from io.EOF", func(t *testing.T) {

		t.Cleanup(s.reset)

		callFailure := fmt.Errorf("RequestError: send request failed")

		failIf = func() (bool, error) {
			return true, callFailure
		}

		lastCheckpoint, err := delta.FindLastCompleteCheckpoint(s, delta.MaxInstance)

		assert.Equal(t, lastCheckpoint, mo.None[*delta.CheckpointInstance]())
		assert.Equal(t, err.Error(), eris.Wrap(callFailure, "").Error())
	})

	t.Run("returns err when any call to iterator fails with an error different from io.EOF", func(t *testing.T) {

		t.Cleanup(s.reset)

		callFailure := fmt.Errorf("RequestError: send request failed")

		var n int
		failIf = func() (bool, error) {
			n++
			if n == 3 {
				return true, callFailure
			}
			return false, nil
		}

		lastCheckpoint, err := delta.FindLastCompleteCheckpoint(s, delta.MaxInstance)

		assert.Equal(t, lastCheckpoint, mo.None[*delta.CheckpointInstance]())
		assert.Equal(t, err.Error(), eris.Wrap(callFailure, "").Error())
	})

	t.Run("returns no err when io.EOF is returned", func(t *testing.T) {

		t.Cleanup(s.reset)

		var n int
		failIf = func() (bool, error) {
			n++
			if n == 2 {
				return true, io.EOF
			}
			return false, nil
		}

		lastCheckpoint, err := delta.FindLastCompleteCheckpoint(s, delta.MaxInstance)

		assert.Equal(t, lastCheckpoint, mo.None[*delta.CheckpointInstance]())
		assert.Equal(t, err, nil)
	})

}

func newMemLogStore() (*memLogStore, error) {
	return &memLogStore{}, nil
}

type memLogStore struct {
	mustFail func() (bool, error)
}

func (s *memLogStore) reset() {
	s.mustFail = func() (bool, error) { return false, nil }
}

func (s *memLogStore) Root() string {
	return ""
}

func (s *memLogStore) Read(path string) (iter.Iter[string], error) {
	return nil, fmt.Errorf("not implemented")
}

var failIf func() (bool, error)

func (s *memLogStore) ListFrom(path string) (iter.Iter[*store.FileMeta], error) {
	return &memIter{
		mustFail: failIf,
	}, nil
}

type memIter struct {
	mustFail func() (bool, error)
}

func (i *memIter) Next() (*store.FileMeta, error) {
	fail, err := i.mustFail()
	if fail {
		return nil, err
	}
	return &store.FileMeta{}, nil
}

func (i *memIter) Close() error {
	return fmt.Errorf("not implemented")
}

func (s *memLogStore) Write(path string, actions iter.Iter[string], overwrite bool) error {
	return fmt.Errorf("not implemented")
}

func (s *memLogStore) ResolvePathOnPhysicalStore(path string) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (s *memLogStore) IsPartialWriteVisible(path string) bool {
	return false
}

func (s *memLogStore) Exists(path string) (bool, error) {
	return false, fmt.Errorf("not implemented")
}
func (s *memLogStore) Create(path string) error {
	return fmt.Errorf("not implemented")
}
