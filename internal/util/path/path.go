package path

import (
	"net/url"
	"strings"

	"github.com/rotisserie/eris"
)

type Config struct {

	// azure blob, this can be controlled by env vars
	// AZURE_STORAGE_DOMAIN, AZURE_STORAGE_PROTOCOL, AZURE_STORAGE_IS_CDN, and AZURE_STORAGE_IS_LOCAL_EMULATOR
	AzureDomain   string
	AzureProtocol string
	AzureCDN      bool
	AzureLocalemu bool
}

// From constructs
// azblob://my-bucket/path/to/log/
// file:///path/to/
// gs://my-bucket/path/to/
func ConvertToBlobURL(urlstr string) (string, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", err
	}

	if len(u.Query()) > 0 {
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
	}

	return "", eris.New("not supported scheme" + u.Scheme)
}
