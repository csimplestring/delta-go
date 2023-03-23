package filenames

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/csimplestring/delta-go/errno"
	"github.com/samber/mo"
)

var checkpointFilePattern = regexp.MustCompile("\\d+\\.checkpoint(\\.\\d+\\.\\d+)?\\.parquet")
var deltaFilePattern = regexp.MustCompile("\\d+\\.json")

func DeltaFile(path string, version int64) string {
	return path + fmt.Sprintf("%020d.json", version)
}

func CheckpointVersion(path string) int64 {
	path = filepath.Base(path)
	v, _ := strconv.ParseInt(strings.Split(path, ".")[0], 10, 64)
	return v
}

func IsCheckpointFile(path string) bool {
	path = filepath.Base(path)
	return checkpointFilePattern.MatchString(path)
}

func DeltaVersion(path string) int64 {
	path = filepath.Base(path)
	v, _ := strconv.ParseInt(strings.TrimSuffix(path, ".json"), 10, 64)
	return v
}

func IsDeltaFile(path string) bool {
	path = filepath.Base(path)
	ret := deltaFilePattern.MatchString(path)
	return ret
}

func GetFileVersion(path string) (int64, error) {
	if IsCheckpointFile(path) {
		return CheckpointVersion(path), nil
	} else if IsDeltaFile(path) {
		return DeltaVersion(path), nil
	} else {
		return -1, errno.UnexpectedFileType(path)
	}
}

func NumCheckpointParts(path string) mo.Option[int] {
	path = filepath.Base(path)
	segments := strings.Split(path, ".")
	if len(segments) != 5 {
		return mo.None[int]()
	}

	n, _ := strconv.ParseInt(segments[3], 10, 32)
	return mo.Some(int(n))
}

func CheckpointPrefix(path string, version int64) string {
	return path + fmt.Sprintf("%020d.checkpoint", version)
}

func CheckpointFileSingular(dir string, version int64) string {
	return dir + fmt.Sprintf("%020d.checkpoint.parquet", version)
}

func CheckpointFileWithParts(dir string, version int64, numParts int) []string {
	res := make([]string, numParts)
	for i := 1; i < numParts+1; i++ {
		res[i-1] = dir + fmt.Sprintf("%020d.checkpoint.%010d.%010d.parquet", version, i, numParts)
	}
	return res
}
