package path

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/csimplestring/delta-go/errno"
	"github.com/rotisserie/eris"
)

func Qualified(base string, path string) (string, error) {
	p, err := url.Parse(base)
	if err != nil {
		return "", eris.Wrap(err, base)
	}

	if p.Scheme == "file" {
		return unixQualified(base, path)
	}
	return "", eris.New(fmt.Sprintf("unsupported scheme %s", p.Scheme))
}

func Relative(base string, path string) (string, error) {
	p, err := url.Parse(base)
	if err != nil {
		return "", eris.Wrap(err, base)
	}

	if p.Scheme == "file" || (len(p.Scheme) == 0 && strings.HasPrefix(base, "/")) {
		return unixRelative(base, path)
	}
	return "", eris.New(fmt.Sprintf("unsupported scheme %s", p.Scheme))
}

func unixQualified(base string, path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	p := filepath.Join(base, path)
	return strings.Replace(p, "file:", "file://", 1), nil
}

func unixRelative(base string, path string) (string, error) {
	base = strings.TrimPrefix(base, "file://")
	path = strings.TrimPrefix(path, "file://")

	// relative path is unchanged
	if !filepath.IsAbs(path) {
		return path, nil
	}

	rel, err := filepath.Rel(base, path)
	if err != nil {
		return "", err
	}

	// converts absolute path to relative path when in table path
	if strings.HasPrefix(rel, "../") {
		return "file://" + path, nil
	}
	// absolute path is unaltered and made fully qualified when not in table path
	return rel, nil
}

func Canonicalize(path string, schema string) (string, error) {

	if schema == "file" {
		return unixCanonicalize(path)
	}

	return "", errno.UnsupportedFileSystem(schema)

}

func unixCanonicalize(p string) (string, error) {
	if strings.HasPrefix(p, "file:///") {
		return p, nil
	}
	if strings.HasPrefix(p, "file:/") {
		return "file:///" + strings.TrimPrefix(p, "file:/"), nil
	}
	if filepath.IsAbs(p) {
		return "file://" + p, nil
	}

	return p, nil
}
