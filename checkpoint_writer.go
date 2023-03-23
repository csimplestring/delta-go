package deltago

import (
	"log"

	"github.com/csimplestring/delta-go/action"
	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	"github.com/rotisserie/eris"
)

type checkpointWriter struct {
	schemaText string
	pw         parquetActionWriter
}

// The scala version passed the DeltaLog instance to infer if we need to use 'rename'
// but here I decide to pass some Config struct in future to instantiate writer.
func (c *checkpointWriter) write(snapshot *snapshotImp) (*CheckpointMetaDataJSON, error) {
	checkpointSize := int64(0)
	numOfFiles := int64(0)

	path := filenames.CheckpointFileSingular(snapshot.path, snapshot.version)

	// exclude CommitInfo and CDC
	actions, err := c.extractActions(snapshot)
	if err != nil {
		return nil, err
	}

	if err := c.pw.Open(path, c.schemaText); err != nil {
		return nil, err
	}

	for _, action := range actions {
		if err := c.pw.Write(action); err != nil {
			return nil, err
		}
		checkpointSize++
		if action.Add != nil {
			numOfFiles++
		}
	}
	if err := c.pw.Close(); err != nil {
		return nil, err
	}

	if n, err := snapshot.numOfFiles(); err == nil {
		if n != numOfFiles {
			return nil, errno.IllegalStateError("State of the checkpoint doesn't match that of the snapshot.")
		}
	}
	if checkpointSize == 0 {
		log.Println("Attempted to write an empty checkpoint without any actions. ")
	}

	return &CheckpointMetaDataJSON{Version: snapshot.version, Size: checkpointSize}, nil
}

func (c *checkpointWriter) extractActions(snapshot *snapshotImp) ([]*action.SingleAction, error) {
	var actions []*action.SingleAction

	// protocol and metadata
	protocolAndMetadata, err := snapshot.protocolAndMetadata.Get()
	if err != nil {
		return nil, eris.Wrap(err, "")
	}
	actions = append(actions, protocolAndMetadata.V2.Wrap(), protocolAndMetadata.V1.Wrap())

	// transaction
	for _, trx := range snapshot.setTransactions() {
		actions = append(actions, trx.Wrap())
	}

	// addFile
	addFiles, err := snapshot.AllFiles()
	if err != nil {
		return nil, err
	}
	for _, f := range addFiles {
		actions = append(actions, f.Wrap())
	}

	// removeFile
	removeFiles, err := snapshot.tombstones()
	if err != nil {
		return nil, err
	}
	for _, f := range removeFiles {
		actions = append(actions, f.Wrap())
	}

	return actions, nil
}
