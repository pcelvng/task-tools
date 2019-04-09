package s3

import (
	"strings"
	"sync"

	minio "github.com/minio/minio-go"
	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/util"
)

var (
	// domain of s3 compatible api
	StoreHost = "s3.amazonaws.com"

	// map that maintains s3 clients
	// to prevent creating new clients with
	// every file for the same auth credentials
	s3Clients = make(map[string]*minio.Client)
	mu        sync.Mutex
)

func NewOptions() *Options {
	return &Options{
		Options: buf.NewOptions(),
	}
}

type Options struct {
	*buf.Options
}

func newS3Client(accessKey, secretKey string) (s3Client *minio.Client, err error) {
	mu.Lock()
	defer mu.Unlock()

	s3Client, _ = s3Clients[StoreHost+accessKey+secretKey]
	if s3Client == nil {
		s3Client, err = minio.New(StoreHost, accessKey, secretKey, true)
		s3Clients[StoreHost+accessKey+secretKey] = s3Client
	}
	return s3Client, err
}

// parsePth will parse an s3 path of the form:
// "s3://{bucket}/{path/to/object.txt}
// and return the bucket and object path.
// If either bucket or object are empty then
// pth was not in the correct format for parsing or
// object and or bucket do not exist in pth.
func parsePth(pth string) (bucket, objPth string) {
	_, bucket, objPth = util.ParsePath(pth)
	objPth = strings.TrimLeft(objPth, "/")
	return bucket, objPth
}
