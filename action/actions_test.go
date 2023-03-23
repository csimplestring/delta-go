package action

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetadata_Equal(t *testing.T) {
	t1 := int64(1)
	t2 := int64(1)

	m1 := &Metadata{
		ID:          "1",
		Name:        "2",
		Description: "3",
		Format: Format{
			Proviver: "4",
			Options: map[string]string{
				"5": "5",
			},
		},
		SchemaString:     "6",
		PartitionColumns: []string{"7"},
		Configuration:    map[string]string{"8": "8"},
		CreatedTime:      &t1,
	}

	m2 := &Metadata{
		ID:          "1",
		Name:        "2",
		Description: "3",
		Format: Format{
			Proviver: "4",
			Options: map[string]string{
				"5": "5",
			},
		},
		SchemaString:     "6",
		PartitionColumns: []string{"7"},
		Configuration:    map[string]string{"8": "8"},
		CreatedTime:      &t2,
	}

	assert.True(t, m1.Equals(m2))
}
