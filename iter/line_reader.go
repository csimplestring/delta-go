package iter

import (
	"bufio"
	"io"
	"strings"

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

func AsReadCloser(itr Iter[string], appendNewline bool) *readCloser {
	return &readCloser{
		iter:          itr,
		buf:           strings.NewReader(""),
		appendNewline: appendNewline,
	}
}

type readCloser struct {
	iter          Iter[string]
	buf           *strings.Reader
	appendNewline bool
}

func (l *readCloser) Read(p []byte) (n int, err error) {
	// remaining data
	if l.buf.Len() > 0 {
		n, err = l.buf.Read(p)
		return
	}

	if !l.iter.Next() {
		return 0, io.EOF
	}

	str, err := l.iter.Value()
	if err != nil {
		return 0, err
	}
	if l.appendNewline {
		str = str + "\n"
	}

	l.buf.Reset(str)
	return l.buf.Read(p)
}

func (l *readCloser) Close() error {
	return l.iter.Close()
}
