package s3

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/pcelvng/task-tools/file/buf"
)

var storeEndpoint = "s3.amazonaws.com"

func NewOptions() *Options {
	return &Options{
		Options: buf.NewOptions(),
	}
}

type Options struct {
	*buf.Options
}

type PathErr string

func (e PathErr) Error() string {
	return fmt.Sprintf("bad s3 path: %v", string(e))
}

type S3Err struct {
	err error // original s3 error
}

func (e *S3Err) Error() string {
	return fmt.Sprintf("s3: %v", e.err.Error())
}

// parsePth will parse an s3 path of the form:
// "s3://{bucket}/{path/to/object.txt}
// and return the bucket and object path.
// If either bucket or object are empty then
// pth was not in the correct format for parsing or
// object and or bucket do not exist in pth.
func parsePth(pth string) (bucket, objPth string) {
	// parse
	pPth, err := url.Parse(pth)
	if err != nil {
		return "", ""
	}

	bucket = pPth.Host
	objPth = strings.TrimLeft(pPth.Path, "/")
	return bucket, objPth
}
