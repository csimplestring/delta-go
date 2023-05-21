package deltago

import (
	"strconv"
	"strings"
	"time"

	"github.com/barweiss/go-tuple"
	"github.com/csimplestring/delta-go/action"
	duration "github.com/xhit/go-str2duration/v2"
)

type Config struct {
	//StorageConfig StorageConfig
	StoreType string
}

// DeltaConfig
type TableConfig[T any] struct {
	Key          string
	DefaultValue string
	FromString   func(s string) T
}

func (t *TableConfig[T]) fromMetadata(metadata *action.Metadata) T {
	if v, ok := metadata.Configuration[t.Key]; ok {
		return t.FromString(v)
	} else {
		return t.FromString(t.DefaultValue)
	}
}

var timeDurationUnits = map[string]string{
	"nanosecond":  "ns",
	"microsecond": "us",
	"millisecond": "ms",
	"second":      "s",
	"hour":        "h",
	"day":         "d",
	"week":        "w",
}

// The string value of this config has to have the following format: interval <number> <unit>.
// Where <unit> is either week, day, hour, second, millisecond, microsecond or nanosecond.
// If it's missing in metadata then the `self.default` is used
func parseDuration(s string) time.Duration {
	fields := strings.Fields(strings.ToLower(s))
	if len(fields) != 3 {
		panic("can't parse duration from string " + s)
	}
	if fields[0] != "interval" {
		panic("this is not a valid duration starting with " + fields[0])
	}

	d, err := duration.ParseDuration(fields[1] + timeDurationUnits[fields[2]])
	if err != nil {
		panic(err)
	}

	return d
}

var DeltaConfigLogRetention = &TableConfig[time.Duration]{
	Key:          "logRetentionDuration",
	DefaultValue: "interval 30 days",
	FromString:   parseDuration,
}

var DeltaConfigTombstoneRetention = &TableConfig[time.Duration]{
	Key:          "deletedFileRetentionDuration",
	DefaultValue: "interval 1 week",
	FromString:   parseDuration,
}

var DeltaConfigCheckpointInterval = &TableConfig[int]{
	Key:          "checkpointInterval",
	DefaultValue: "10",
	FromString: func(s string) int {
		i, _ := strconv.Atoi(s)
		return i
	},
}

var DeltaConfigEnableExpiredLogCleanup = &TableConfig[bool]{
	Key:          "enableExpiredLogCleanup",
	DefaultValue: "true",
	FromString: func(s string) bool {
		return strings.ToLower(s) == "true"
	},
}

var DeltaConfigIsAppendOnly = &TableConfig[bool]{
	Key:          "appendOnly",
	DefaultValue: "false",
	FromString: func(s string) bool {
		return strings.ToLower(s) == "true"
	},
}

type tableConfigurations []*tuple.T2[string, string]

func mergeGlobalTableConfigurations(confs tableConfigurations, tableConf map[string]string) map[string]string {

	res := make(map[string]string, len(tableConf)+len(confs))
	for k, v := range tableConf {
		res[k] = v
	}

	for _, v := range confs {
		if _, ok := res[v.V1]; !ok {
			res[v.V1] = v.V2
		}
	}
	return res
}
