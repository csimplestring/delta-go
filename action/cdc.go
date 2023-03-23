package action

import (
	"net/url"
)

type AddCDCFile struct {
	Path            string            `json:"path,omitempty"`
	DataChange      bool              `json:"dataChange,omitempty"`
	PartitionValues map[string]string `json:"partitionValues,omitempty"`
	Size            int64             `json:"size,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
}

func (a *AddCDCFile) IsDataChanged() bool {
	return a.DataChange
}

func (a *AddCDCFile) PathAsUri() (*url.URL, error) {
	return url.Parse(a.Path)
}

func (a *AddCDCFile) Wrap() *SingleAction {
	return &SingleAction{Cdc: a}
}

func (a *AddCDCFile) Json() (string, error) {
	return jsonString(a)
}
