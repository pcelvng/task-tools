package gcs

import (
	"strings"
	"sync"

	minio "github.com/minio/minio-go"
	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/util"
)

var (
	// StoreHost is a GCS endpoint (s3 compatible api)
	StoreHost = "storage.googleapis.com"

	// map that maintains gcs clients
	// to prevent creating new clients with
	// every file for the same auth credentials
	gcsClients = make(map[string]*minio.Client)
	mu         sync.Mutex
)

func NewOptions() *Options {
	return &Options{
		Options: buf.NewOptions(),
	}
}

type Options struct {
	*buf.Options
}

func newGCSClient(accessKey, secretKey string) (gcsClient *minio.Client, err error) {
	mu.Lock()
	defer mu.Unlock()

	gcsClient, _ = gcsClients[StoreHost+accessKey+secretKey]
	if gcsClient == nil {
		gcsClient, err = minio.New(StoreHost, accessKey, secretKey, true)
		gcsClients[StoreHost+accessKey+secretKey] = gcsClient
	}
	return gcsClient, err
}

// parsePth will parse an gcs path of the form:
// "gcs://{bucket}/{path/to/object.txt}
// and return the bucket and object path.
// If either bucket or object are empty then
// pth was not in the correct format for parsing or
// object and or bucket do not exist in pth.
func parsePth(pth string) (bucket, objPth string) {
	_, bucket, objPth = util.ParsePath(pth)
	objPth = strings.TrimLeft(objPth, "/")
	return bucket, objPth
}
