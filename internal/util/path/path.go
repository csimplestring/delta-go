package path

import (
	"net/url"
	"os"
	"strings"

	"github.com/rotisserie/eris"
)

// From constructs
// azblob://my-bucket/path/to/log/
// file:///path/to/
// gs://my-bucket/path/to/
func ConvertToBlobURL(urlstr string) (string, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", err
	}

	if len(u.Query()) > 0 && u.Scheme != "s3" {
		return "", eris.New("path url cannot have query parameters!")
	}

	v := url.Values{}
	if u.Scheme == "file" {
		v.Set("metadata", "skip")
		v.Set("create_dir", "true")
		u.RawQuery = v.Encode()
		return u.String(), nil
	} else if u.Scheme == "azblob" || u.Scheme == "gs" {
		bucket, prefix, found := strings.Cut(u.Path, "/")
		u.Path = bucket
		// set prefix
		if found {
			v.Set("prefix", prefix)
		}

		q, err := url.QueryUnescape(v.Encode())
		if err != nil {
			return "", eris.Wrap(err, "")
		}
		u.RawQuery = q
		return u.String(), nil
	} else if u.Scheme == "s3" {
		bucket, prefix, found := strings.Cut(u.Path, "/")
		u.Path = bucket
		// set prefix
		if found {
			v.Set("prefix", prefix)
		}
		// set the endpoint url if localstack is used
		if endpoint, ok := os.LookupEnv("AWS_ENDPOINT_URL"); ok {
			v.Set("endpoint", endpoint)
		}
		if disableSSL, ok := os.LookupEnv("AWS_DISABLE_SSL"); ok {
			v.Set("disableSSL", disableSSL)
		}
		if s3ForcePathStyle, ok := os.LookupEnv("AWS_S3_FORCE_PATH_STYLE"); ok {
			v.Set("s3ForcePathStyle", s3ForcePathStyle)
		}

		q, err := url.QueryUnescape(v.Encode())
		if err != nil {
			return "", eris.Wrap(err, "")
		}
		u.RawQuery = q
		return u.String(), nil
	}

	return "", eris.New("not supported scheme" + u.Scheme)
}
