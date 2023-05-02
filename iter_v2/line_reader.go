package iter_v2

import (
	"bufio"
	"io"
	"strings"
)

var _ Iter[string] = &LineReader{}

// FromReadCloser returns an iterator producing lines from the given reader.
func FromReadCloser(r io.ReadCloser) *LineReader {
	s := *bufio.NewScanner(r)

	return &LineReader{
		reader:  r,
		scanner: s,
	}
}

type LineReader struct {
	reader  io.ReadCloser
	scanner bufio.Scanner
}

// Value implements Iterator[T].Value by returning the next line from the
// reader.
func (it *LineReader) Next() (string, error) {

	if it.scanner.Scan() {
		s := it.scanner.Text()
		err := it.scanner.Err()
		return s, err
	}

	return "", io.EOF
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

	str, err := l.iter.Next()
	if err == io.EOF {
		return 0, io.EOF
	}
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
