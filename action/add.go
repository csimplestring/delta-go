package action

import (
	"net/url"
	"time"

	"github.com/ulule/deepcopier"
)

type AddFile struct {
	Path             string            `json:"path,omitempty"`
	DataChange       bool              `json:"dataChange,omitempty"`
	PartitionValues  map[string]string `json:"partitionValues,omitempty"`
	Size             int64             `json:"size,omitempty"`
	ModificationTime int64             `json:"modificationTime,omitempty"`
	Stats            string            `json:"stats,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

func (a *AddFile) IsDataChanged() bool {
	return a.DataChange
}

func (a *AddFile) PathAsUri() (*url.URL, error) {
	return url.Parse(a.Path)
}

func (a *AddFile) Wrap() *SingleAction {
	return &SingleAction{Add: a}
}

func (a *AddFile) Json() (string, error) {
	return jsonString(a)
}

func (a *AddFile) Remove() *RemoveFile {
	return a.RemoveWithTimestamp(nil, nil)
}

func (a *AddFile) RemoveWithTimestamp(ts *int64, dataChange *bool) *RemoveFile {
	if ts == nil {
		*ts = time.Now().UnixMilli()
	}
	if dataChange == nil {
		*dataChange = true
	}

	return &RemoveFile{
		Path:              a.Path,
		DeletionTimestamp: ts,
		DataChange:        *dataChange,
	}
}

func (a *AddFile) Copy(dataChange bool, path string) *AddFile {
	dst := &AddFile{}
	deepcopier.Copy(a).To(dst)
	dst.Path = path
	dst.DataChange = dataChange
	return dst
}
