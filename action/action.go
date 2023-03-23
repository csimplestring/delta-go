package action

import (
	"encoding/json"
	"net/url"

	"github.com/csimplestring/delta-go/errno"
)

const ReaderVersion = 1
const WriterVersion = 2
const MinReaderVersionProp = "delta.minReaderVersion"
const MinWriterVersionProp = "delta.minWriterVersion"

type Action interface {
	Wrap() *SingleAction
	Json() (string, error)
}

type FileAction interface {
	Action
	PathAsUri() (*url.URL, error)
	IsDataChanged() bool
}

func FromJson(s string) (Action, error) {
	action := &SingleAction{}
	if err := json.Unmarshal([]byte(s), action); err != nil {
		return nil, errno.JsonUnmarshalError(err)
	}

	return action.Unwrap(), nil
}

func jsonString(a Action) (string, error) {
	b, err := json.Marshal(a.Wrap())
	if err != nil {
		return "", errno.JsonMarshalError(err)
	}
	return string(b), nil
}

func CheckMetadataProtocolProperties(metadata *Metadata, protocol *Protocol) error {
	if _, ok := metadata.Configuration[MinReaderVersionProp]; ok {
		return errno.AssertionError("should not have the protocol version MinReaderVersion as part of the table properties")
	}
	if _, ok := metadata.Configuration[MinWriterVersionProp]; ok {
		return errno.AssertionError("should not have the protocol version MinWriterVersion as part of the table properties")
	}
	return nil
}

type SingleAction struct {
	Txn        *SetTransaction `json:"txn,omitempty"`
	Add        *AddFile        `json:"add,omitempty"`
	Remove     *RemoveFile     `json:"remove,omitempty"`
	MetaData   *Metadata       `json:"metaData,omitempty"`
	Protocol   *Protocol       `json:"protocol,omitempty"`
	Cdc        *AddCDCFile     `json:"cdc,omitempty"`
	CommitInfo *CommitInfo     `json:"commitInfo,omitempty"`
}

func (s *SingleAction) Unwrap() Action {
	if s.Add != nil {
		return s.Add
	} else if s.Remove != nil {
		return s.Remove
	} else if s.MetaData != nil {
		return s.MetaData
	} else if s.Txn != nil {
		return s.Txn
	} else if s.Protocol != nil {
		return s.Protocol
	} else if s.Cdc != nil {
		return s.Cdc
	} else if s.CommitInfo != nil {
		return s.CommitInfo
	} else {
		return nil
	}
}
