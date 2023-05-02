package deltago

import (
	"encoding/json"
	"io"
	"log"
	"sort"
	"time"

	"github.com/csimplestring/delta-go/errno"
	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/internal/util/filenames"
	iter "github.com/csimplestring/delta-go/iter_v2"
	"github.com/csimplestring/delta-go/store"
	"github.com/rotisserie/eris"

	"github.com/samber/mo"
)

const LastCheckpointPath string = "_last_checkpoint"

type CheckpointMetaDataJSON struct {
	Version int64 `json:"version,omitempty"`
	Size    int64 `json:"size,omitempty"`
	Parts   *int  `json:"parts,omitempty"`
}

var MaxInstance = CheckpointInstance{Version: -1, NumParts: mo.None[int]()}

type CheckpointInstance struct {
	Version  int64
	NumParts mo.Option[int]
}

func (t *CheckpointInstance) Compare(other CheckpointInstance) int {
	if t.Version == other.Version {
		return t.NumParts.OrElse(1) - other.NumParts.OrElse(1)
	}
	if t.Version-other.Version < 0 {
		return -1
	} else {
		return 1
	}
}

func (t *CheckpointInstance) IsEarlierThan(other CheckpointInstance) bool {
	if other.Compare(MaxInstance) == 0 {
		return true
	}

	if t.NumParts.IsAbsent() {
		return t.Version <= other.Version
	} else {
		return t.Version < other.Version || (t.Version == other.Version && t.NumParts.MustGet() < other.NumParts.OrElse(1))
	}
}

func (t *CheckpointInstance) IsNotLaterThan(other CheckpointInstance) bool {
	if other.Compare(MaxInstance) == 0 {
		return true
	}
	return t.Version <= other.Version
}

func (t *CheckpointInstance) GetCorrespondingFiles(dir string) (res []string) {
	if t.NumParts.IsAbsent() {
		return []string{filenames.CheckpointFileSingular(dir, t.Version)}
	} else {
		return filenames.CheckpointFileWithParts(dir, t.Version, t.NumParts.MustGet())
	}
}

func FromPath(path string) *CheckpointInstance {
	version := filenames.CheckpointVersion(path)
	numParts := filenames.NumCheckpointParts(path)

	return &CheckpointInstance{Version: version, NumParts: numParts}
}

func FromMetadata(metadata CheckpointMetaDataJSON) *CheckpointInstance {
	i := &CheckpointInstance{Version: metadata.Version}
	if metadata.Parts == nil {
		i.NumParts = mo.None[int]()
	} else {
		i.NumParts = mo.Some(*metadata.Parts)
	}

	return i
}

func LastCheckpoint(s store.Store) (mo.Option[*CheckpointMetaDataJSON], error) {
	return LoadMetadataFromFile(s)
}

func LoadMetadataFromFile(s store.Store) (mo.Option[*CheckpointMetaDataJSON], error) {

	for i := 0; i < 3; i++ {
		if i != 0 {
			time.Sleep(time.Second)
		}

		lines, err := s.Read(LastCheckpointPath)
		if err != nil {
			if eris.Is(err, errno.ErrFileNotFound) {
				return mo.None[*CheckpointMetaDataJSON](), nil
			} else {
				continue
			}
		}

		line, err := lines.Next()

		if err == io.EOF {
			log.Println("failed to read last checkpoint, end of iterator, try again")
			continue
		}
		if err != nil {
			log.Println("failed to get line from iterator when reading last checkpoint, try again")
			lines.Close()
			continue
		}

		res := &CheckpointMetaDataJSON{}
		err = json.Unmarshal([]byte(line), res)
		if err != nil {
			log.Println("failed to unmarshal json line when reading last checkpoint, try again")
			continue
		}
		return mo.Some(res), nil
	}

	// tried 3 times, still failed, can not find last_checkpoint
	// Hit a partial file. This could happen on Azure as overwriting _last_checkpoint file is
	// not atomic. We will try to list all files to find the latest checkpoint and restore
	// CheckpointMetaData from it.
	if lastCheckpoint, err := FindLastCompleteCheckpoint(s, MaxInstance); err != nil {
		return mo.None[*CheckpointMetaDataJSON](), eris.Wrap(err, "FindLastCompleteCheckpoint")
	} else {
		return manuallyLoadCheckpoint(lastCheckpoint), nil
	}
}

func manuallyLoadCheckpoint(t mo.Option[*CheckpointInstance]) mo.Option[*CheckpointMetaDataJSON] {
	if t.IsAbsent() {
		return mo.None[*CheckpointMetaDataJSON]()
	}

	cv := t.MustGet()
	if p, ok := cv.NumParts.Get(); ok {
		return mo.Some(&CheckpointMetaDataJSON{Version: cv.Version, Size: -1, Parts: &p})
	} else {
		return mo.Some(&CheckpointMetaDataJSON{Version: cv.Version, Size: -1})
	}
}

func FindLastCompleteCheckpoint(s store.Store, cv CheckpointInstance) (mo.Option[*CheckpointInstance], error) {

	cur := util.MaxInt64(cv.Version, 0)
	for cur >= 0 {

		iter, err := s.ListFrom(filenames.CheckpointPrefix(s.Root(), util.MaxInt64(0, cur-1000)))
		if err != nil {
			return mo.None[*CheckpointInstance](), eris.Wrap(err, "")
		}

		var checkpoints []*CheckpointInstance
		for f, err := iter.Next(); err == nil; f, err = iter.Next() {

			if !filenames.IsCheckpointFile(f.Path()) {
				continue
			}
			cp := FromPath(f.Path())
			if cur == 0 || cp.Version <= cur || cp.IsEarlierThan(cv) {
				checkpoints = append(checkpoints, cp)
			} else {
				break
			}
		}
		if err != nil && err != io.EOF {
			return mo.None[*CheckpointInstance](), eris.Wrap(err, "")
		}

		lastCheckpoint := GetLatestCompleteCheckpointFromList(checkpoints, cv)
		if lastCheckpoint.IsPresent() {
			return lastCheckpoint, nil
		} else {
			cur -= 1000
		}
	}

	return mo.None[*CheckpointInstance](), nil
}

type instanceKey struct {
	Version  int64
	NumParts int
	HasParts bool
}

func (i instanceKey) toInstance() *CheckpointInstance {
	if i.HasParts {
		return &CheckpointInstance{Version: i.Version, NumParts: mo.Some(i.NumParts)}
	}
	return &CheckpointInstance{Version: i.Version, NumParts: mo.None[int]()}
}

func GetLatestCompleteCheckpointFromList(instances []*CheckpointInstance, notLaterThan CheckpointInstance) mo.Option[*CheckpointInstance] {

	grouped := make(map[instanceKey][]*CheckpointInstance)
	for _, i := range instances {
		if !i.IsNotLaterThan(notLaterThan) {
			continue
		}
		k := instanceKey{Version: i.Version, NumParts: i.NumParts.OrElse(1), HasParts: i.NumParts.IsPresent()}
		if vals, ok := grouped[k]; ok {
			vals = append(vals, i)
			grouped[k] = vals
		} else {
			grouped[k] = []*CheckpointInstance{i}
		}
	}

	var res []*CheckpointInstance
	for k, v := range grouped {
		if k.NumParts != len(v) {
			continue
		}
		res = append(res, k.toInstance())
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Compare(*res[j]) < 0
	})

	if len(res) != 0 {
		return mo.Some(res[len(res)-1])
	}
	return mo.None[*CheckpointInstance]()
}

func checkpoint(store store.Store, snapshotToCheckpoint *snapshotImp) error {
	//var checkpointWriter *checkpointWriter
	wc := parquetActionWriterConfig{}
	storageType := snapshotToCheckpoint.config.StorageConfig.Scheme
	if storageType == Local {
		wc.Local = &parquetActionLocalWriterConfig{}
	} else {
		panic("unsupported storage type")
	}

	writer := &checkpointWriter{
		schemaText: actionSchemaDefinitionString,
		pw:         newParquetActionWriter(&wc),
	}

	checkpointMetadata, err := writer.write(snapshotToCheckpoint)
	if err != nil {
		return err
	}

	b, err := json.Marshal(checkpointMetadata)
	if err != nil {
		return errno.JsonMarshalError(err)
	}

	if err := store.Write(LastCheckpointPath, iter.FromSlice([]string{string(b)}), true); err != nil {
		return err
	}

	// todo: doLogCleanup()

	return nil
}
