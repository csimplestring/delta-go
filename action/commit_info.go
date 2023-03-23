package action

import (
	"github.com/ulule/deepcopier"
)

type CommitMarker interface {
	GetTimestamp() int64
	WithTimestamp(timestamp int64) CommitMarker
	GetVersion() int64
}

type CommitInfo struct {
	Version             *int64            `json:"version,omitempty"`
	Timestamp           int64             `json:"timestamp,omitempty"`
	UserID              *string           `json:"userId,omitempty"`
	UserName            *string           `json:"userName,omitempty"`
	Operation           string            `json:"operation,omitempty"`
	OperationParameters map[string]string `json:"operationParameters,omitempty"`
	Job                 *JobInfo          `json:"job,omitempty"`
	Notebook            *NotebookInfo     `json:"notebook,omitempty"`
	ClusterId           *string           `json:"clusterId,omitempty"`
	ReadVersion         *int64            `json:"readVersion,omitempty"`
	IsolationLevel      *string           `json:"isolationLevel,omitempty"`
	IsBlindAppend       *bool             `json:"isBlindAppend,omitempty"`
	OperationMetrics    map[string]string `json:"operationMetrics,omitempty"`
	UserMetadata        *string           `json:"userMetadata,omitempty"`
	EngineInfo          *string           `json:"engineInfo,omitempty"`
}

func (c *CommitInfo) Wrap() *SingleAction {
	return &SingleAction{CommitInfo: c}
}

func (c *CommitInfo) Json() (string, error) {
	return jsonString(c)
}

func (c *CommitInfo) GetTimestamp() int64 {
	return c.Timestamp
}

func (c *CommitInfo) WithTimestamp(timestamp int64) CommitMarker {
	copied := &CommitInfo{}
	deepcopier.Copy(c).To(copied)

	copied.Timestamp = timestamp
	return copied
}

func (c *CommitInfo) GetVersion() int64 {
	return *c.Version
}

func (c *CommitInfo) Copy(version int64) *CommitInfo {
	res := &CommitInfo{}
	deepcopier.Copy(c).To(res)
	res.Version = &version
	return res
}
