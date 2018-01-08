package s3

import (
	"fmt"
	"net/url"
)

func NewOptions() *Options {
	return &Options{}
}

type Options struct {
	// UseFileBuf specifies to use a tmp file for the delayed writing.
	// Can optionally also specify the tmp directory and tmp name
	// prefix.
	UseFileBuf bool

	// FileBufDir optionally specifies the temp directory. If not specified then
	// the os default temp dir is used.
	FileBufDir string

	// FileBufPrefix optionally specifies the temp file prefix.
	// The full tmp file name is randomly generated and guaranteed
	// not to conflict with existing files. A prefix can help one find
	// the tmp file.
	FileBufPrefix string
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
func parsePth(pth string) (bucket, objPth string, err error) {
	// parse
	pPth, err := url.Parse(pth)
	if err != nil {
		return "", "", PathErr(pth)
	}

	scheme := pPth.Scheme
	bucket = pPth.Host
	objPth = pPth.Path

	if scheme != "s3" || bucket == "" || objPth == "" {
		return "", "", PathErr(pth)
	}
	return bucket, objPth, nil
}
