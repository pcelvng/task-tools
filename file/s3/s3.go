package s3

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

// Stat a s3 directory or file for additional information
func Stat(pth string, accessKey, secretKey string) (stat.Stats, error) {
	client, err := newS3Client(accessKey, secretKey)
	if err != nil {
		return stat.Stats{}, errors.Wrap(err, "s3 client init")
	}
	bucket, objPth := parsePth(pth)
	info, err := client.StatObject(bucket, objPth, minio.StatObjectOptions{})
	// check if directory
	if err != nil {
		donech := make(chan struct{})
		defer close(donech)
		count := 0
		for range client.ListObjects(bucket, objPth, false, donech) {
			count++
		}
		if count > 0 {
			return stat.Stats{
				Path:  pth,
				IsDir: true,
			}, nil
		}
		return stat.Stats{}, err
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
