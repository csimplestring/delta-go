package deltago

import (
	"github.com/csimplestring/delta-go/action"
	iter "github.com/csimplestring/delta-go/iter_v2"
	"github.com/csimplestring/delta-go/store"
)

type VersionLog interface {
	Version() int64
	Actions() ([]action.Action, error)
	ActionIter() (iter.Iter[action.Action], error)
}

var _ VersionLog = &InMemVersionLog{}
var _ VersionLog = &MemOptimizedVersionLog{}

type InMemVersionLog struct {
	version int64
	actions []action.Action
}

func (v *InMemVersionLog) Version() int64 {
	return v.version
}

func (v *InMemVersionLog) Actions() ([]action.Action, error) {
	return v.actions, nil
}

func (v *InMemVersionLog) ActionIter() (iter.Iter[action.Action], error) {
	return iter.FromSlice(v.actions), nil
}

type MemOptimizedVersionLog struct {
	version int64
	path    string
	store   store.Store
}

func (m *MemOptimizedVersionLog) Version() int64 {
	return m.version
}

func (m *MemOptimizedVersionLog) Actions() ([]action.Action, error) {
	i, err := m.store.Read(m.path)
	if err != nil {
		return nil, err
	}
	defer i.Close()

	return iter.Map(i, func(t string) (action.Action, error) {
		return action.FromJson(t)
	})
}

func (m *MemOptimizedVersionLog) ActionIter() (iter.Iter[action.Action], error) {
	i, err := m.store.Read(m.path)
	if err != nil {
		return nil, err
	}

	mapIter := &iter.MapIter[string, action.Action]{
		It: i,
		Mapper: func(s string) (action.Action, error) {
			return action.FromJson(s)
		},
	}
	return mapIter, nil
}
