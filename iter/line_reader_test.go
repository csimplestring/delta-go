package iter

import (
	"fmt"
	"io"
	"os"
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

func TestFromReadCloser(t *testing.T) {
	f, err := os.Open("../tests/golden/snapshot-data0/_delta_log/00000000000000000000.json")
	assert.NoError(t, err)

	iter := FromReadCloser(f)
	for line, err := iter.Next(); err == nil; line, err = iter.Next() {
		fmt.Println(line)
	}
}
