package iter

import (
	"bufio"
	"io"

	"github.com/rotisserie/eris"
)

var _ Iter[string] = &LineReader{}

// FromReadCloser returns an iterator producing lines from the given reader.
func FromReadCloser(r io.ReadCloser) *LineReader {
	s := *bufio.NewScanner(r)
	// advance the line
	eof := s.Scan()

	return &LineReader{
		err:     s.Err(),
		eof:     eof,
		reader:  r,
		scanner: s,
	}
}

type LineReader struct {
	err     error
	eof     bool
	reader  io.ReadCloser
	scanner bufio.Scanner
}

// Next implements Iterator[T].Next.
func (it *LineReader) Next() bool {
	return it.eof
}

// Value implements Iterator[T].Value by returning the next line from the
// reader.
func (it *LineReader) Value() (string, error) {
	if it.err != nil {
		return "", eris.Wrap(it.err, "scanner scan error")
	}

	s := it.scanner.Text()
	it.eof = it.scanner.Scan()
	it.err = it.scanner.Err()

	return s, nil
}

func (it *LineReader) Close() error {
	return it.reader.Close()
}
