package deltago

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSystemClock_WaitTill(t *testing.T) {
	s := &SystemClock{}
	now := time.Now()

	r := s.WaitTill(now.Add(time.Second * 5).UnixMilli())

	diff := (r - now.UnixMilli()) / 1000
	assert.GreaterOrEqual(t, diff, int64(5))
	assert.LessOrEqual(t, diff, int64(6))
}

func TestHashStruct(t *testing.T) {

}
