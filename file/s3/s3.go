package s3

import (
	"net/url"
	"strings"

	"github.com/pcelvng/task-tools/file/buf"
)

var StoreHost = "s3.amazonaws.com"

func NewOptions() *Options {
	return &Options{
		Options: buf.NewOptions(),
	}
}

type Options struct {
	*buf.Options
}

// parsePth will parse an s3 path of the form:
// "s3://{bucket}/{path/to/object.txt}
// and return the bucket and object path.
// If either bucket or object are empty then
// pth was not in the correct format for parsing or
// object and or bucket do not exist in pth.
func parsePth(pth string) (bucket, objPth string) {
	// err is not possible since it's not via a request.
	pPth, _ := url.Parse(pth)
	bucket = pPth.Host
	objPth = strings.TrimLeft(pPth.Path, "/")
	return bucket, objPth
}
