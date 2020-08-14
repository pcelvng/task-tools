package gs

import (
	"strings"
	"sync"
	"time"

	minio "github.com/minio/minio-go"
	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/file/util"
	"github.com/pkg/errors"
)

var (
	// StoreHost is a gs endpoint (s3 compatible api)
	StoreHost = "storage.googleapis.com"

	// map that maintains gs clients
	// to prevent creating new clients with
	// every file for the same auth credentials
	gsClients = make(map[string]*minio.Client)
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

func newGSClient(accessKey, secretKey string) (gsClient *minio.Client, err error) {
	mu.Lock()
	defer mu.Unlock()

	gsClient, _ = gsClients[StoreHost+accessKey+secretKey]
	if gsClient == nil {
		gsClient, err = minio.New(StoreHost, accessKey, secretKey, true)
		gsClients[StoreHost+accessKey+secretKey] = gsClient
	}
	return gsClient, err
}

// parsePth will parse an gs path of the form:
// "gs://{bucket}/{path/to/object.txt}
// and return the bucket and object path.
// If either bucket or object are empty then
// pth was not in the correct format for parsing or
// object and or bucket do not exist in pth.
func parsePth(pth string) (bucket, objPth string) {
	_, bucket, objPth = util.ParsePath(pth)
	objPth = strings.TrimLeft(objPth, "/")
	return bucket, objPth
}

// Stat a s3 directory or file for additional information
func Stat(pth string, accessKey, secretKey string) (stat.Stats, error) {
	client, err := newGSClient(accessKey, secretKey)
	if err != nil {
		return stat.Stats{}, errors.Wrap(err, "s3 client init")
	}
	bucket, objPth := parsePth(pth)

	donech := make(chan struct{})
	defer close(donech)
	count := 0
	var info minio.ObjectInfo
	for info = range client.ListObjects(bucket, objPth, false, donech) {
		if info.Err != nil {
			return stat.Stats{}, err
		}
		count++
	}
	if count > 1 {
		return stat.Stats{
			Path:  pth,
			IsDir: true,
		}, nil
	}

	return stat.Stats{
		ByteCnt:  0,
		Size:     info.Size,
		Checksum: info.ETag,
		Path:     info.Key,
		Created:  info.LastModified.Format(time.RFC3339),
		IsDir:    false,
	}, nil
}
