package deltago

import (
	"fmt"
	"io"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/internal/util/path"
	iter "github.com/csimplestring/delta-go/iter_v2"
)

type InMemoryLogReplay struct {
	storageConfig             StorageConfig
	MinFileRetentionTimestamp int64

	currentProtocolVersion *action.Protocol
	currentVersion         int64
	currentMetaData        *action.Metadata
	sizeInBytes            int64
	numMetadata            int64
	numProtocol            int64
	transactions           map[string]*action.SetTransaction
	activeFiles            map[string]*action.AddFile
	tombstones             map[string]*action.RemoveFile
}

func NewInMemoryLogReplayer(minFileRetentionTimestamp int64, storageConfig StorageConfig) *InMemoryLogReplay {
	return &InMemoryLogReplay{
		MinFileRetentionTimestamp: minFileRetentionTimestamp,
		storageConfig:             storageConfig,
		transactions:              make(map[string]*action.SetTransaction),
		activeFiles:               make(map[string]*action.AddFile),
		tombstones:                make(map[string]*action.RemoveFile),
	}
}

func (r *InMemoryLogReplay) GetSetTransactions() []*action.SetTransaction {
	values := make([]*action.SetTransaction, 0, len(r.transactions))
	for _, v := range r.transactions {
		values = append(values, v)
	}
	return values
}

func (r *InMemoryLogReplay) GetActiveFiles() iter.Iter[*action.AddFile] {
	values := make([]*action.AddFile, 0, len(r.activeFiles))
	for _, v := range r.activeFiles {
		values = append(values, v)
	}
	return iter.FromSlice(values)
}

func (r *InMemoryLogReplay) GetTombstones() iter.Iter[*action.RemoveFile] {
	values := make([]*action.RemoveFile, 0, len(r.tombstones))
	for _, v := range r.tombstones {
		if v.DelTimestamp() > r.MinFileRetentionTimestamp {
			values = append(values, v)
		}
	}
	return iter.FromSlice(values)
}

func (r *InMemoryLogReplay) Append(version int64, iter iter.Iter[action.Action]) error {
	if r.currentVersion == -1 || version == r.currentVersion+1 {
		panic(fmt.Errorf("attempted to replay version %d, but state is at %d", version, r.currentVersion))
	}
	r.currentVersion = version

	for {
		a, err := iter.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		switch v := a.(type) {
		case *action.SetTransaction:
			r.transactions[v.AppId] = v
		case *action.Metadata:
			r.currentMetaData = v
			r.numMetadata += 1
		case *action.Protocol:
			r.currentProtocolVersion = v
			r.numProtocol += 1
		case *action.AddFile:

			canonicalPath, err := path.Canonicalize(v.Path, string(r.storageConfig.Scheme))
			if err != nil {
				return err
			}
			canonicalizedAdd := v.Copy(false, canonicalPath)

			r.activeFiles[canonicalPath] = canonicalizedAdd
			delete(r.tombstones, canonicalPath)
			r.sizeInBytes += canonicalizedAdd.Size
		case *action.RemoveFile:
			canonicalPath, err := path.Canonicalize(v.Path, string(r.storageConfig.Scheme))
			if err != nil {
				return err
			}
			canonicalizedRemove := v.Copy(false, canonicalPath)

			if removeFile, ok := r.activeFiles[canonicalPath]; ok {
				delete(r.activeFiles, canonicalPath)
				r.sizeInBytes -= removeFile.Size
			}
			r.tombstones[canonicalPath] = canonicalizedRemove
		default:
			{
			}
		}
	}

	return iter.Close()
}
