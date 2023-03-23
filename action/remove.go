package action

import (
	"net/url"

	"github.com/ulule/deepcopier"
)

type RemoveFile struct {
	Path                 string            `json:"path,omitempty"`
	DataChange           bool              `json:"dataChange,omitempty"`
	DeletionTimestamp    *int64            `json:"deletionTimestamp,omitempty"`
	ExtendedFileMetadata bool              `json:"extendedFileMetadata,omitempty"`
	PartitionValues      map[string]string `json:"partitionValues,omitempty"`
	Size                 *int64            `json:"size,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
}

func (r *RemoveFile) IsDataChanged() bool {
	return r.DataChange
}

func (r *RemoveFile) PathAsUri() (*url.URL, error) {
	return url.Parse(r.Path)
}

func (r *RemoveFile) Wrap() *SingleAction {
	return &SingleAction{Remove: r}
}

func (r *RemoveFile) Json() (string, error) {
	return jsonString(r)
}

func (r *RemoveFile) DelTimestamp() int64 {
	if r.DeletionTimestamp == nil {
		return 0
	}
	return *r.DeletionTimestamp
}

func (r *RemoveFile) Copy(dataChange bool, path string) *RemoveFile {
	dst := &RemoveFile{}
	deepcopier.Copy(r).To(dst)
	dst.Path = path
	dst.DataChange = dataChange
	return dst
}
