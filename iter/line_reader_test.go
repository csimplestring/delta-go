package iter

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAsReader(t *testing.T) {
	input := []string{
		"abc",
		"f",
		"fejfjewijfoiejfijejejejew",
	}
	iter := FromSlice(input)

	r := AsReadCloser(iter, true)
	buf := make([]byte, 7)
	sb := &strings.Builder{}

	for {
		n, err := r.Read(buf)
		if err == io.EOF {
			break
		}
		sb.Write(buf[:n])
	}
	assert.Equal(t, strings.Join(input, "\n")+"\n", sb.String())
}
