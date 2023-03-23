package action

import (
	"time"

	"github.com/csimplestring/delta-go/internal/util"
	"github.com/csimplestring/delta-go/types"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/repeale/fp-go"
)

type Metadata struct {
	ID               string            `json:"id,omitempty"`
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	Format           Format            `json:"format,omitempty"`
	SchemaString     string            `json:"schemaString,omitempty"`
	PartitionColumns []string          `json:"partitionColumns,omitempty"`
	Configuration    map[string]string `json:"configuration,omitempty"`
	CreatedTime      *int64            `json:"createdTime,omitempty"`
}

func DefaultMetadata() *Metadata {
	now := time.Now().UnixMilli()

	return &Metadata{
		ID:            uuid.New().String(),
		Format:        Format{Proviver: "parquet", Options: map[string]string{}},
		Configuration: map[string]string{},
		CreatedTime:   &now,
	}
}

func (m *Metadata) Wrap() *SingleAction {
	return &SingleAction{MetaData: m}
}

func (m *Metadata) Json() (string, error) {
	return jsonString(m)
}

func (m *Metadata) Schema() (*types.StructType, error) {
	if len(m.SchemaString) == 0 {
		return types.NewStructType(make([]*types.StructField, 0)), nil
	}

	if dt, err := types.FromJSON(m.SchemaString); err != nil {
		return nil, err
	} else {
		return dt.(*types.StructType), nil
	}
}

func (m *Metadata) PartitionSchema() (*types.StructType, error) {
	schema, err := m.Schema()
	if err != nil {
		return nil, err
	}

	var fields []*types.StructField
	for _, c := range m.PartitionColumns {
		if f, err := schema.Get(c); err != nil {
			return nil, err
		} else {
			fields = append(fields, f)
		}
	}
	return types.NewStructType(fields), nil
}

func (m *Metadata) DataSchema() (*types.StructType, error) {
	partitions := mapset.NewSet(m.PartitionColumns...)
	s, err := m.Schema()
	if err != nil {
		return nil, err
	}

	fields := fp.Filter(func(f *types.StructField) bool {
		return !partitions.Contains(f.Name)
	})(s.GetFields())

	return types.NewStructType(fields), nil
}

func (m *Metadata) Equals(other *Metadata) bool {
	return util.MustHash(m) == util.MustHash(other)
}
