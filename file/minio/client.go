package minio

import (
	"net/url"
	"strings"
	"sync"
	"time"

	minio "github.com/minio/minio-go"
	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pkg/errors"
)

var (
	// domain of s3 compatible api
	//	StoreHost = "s3.amazonaws.com"

	// map that maintains minIO clients
	// to prevent creating new clients with
	// every file for the same auth credentials
	minIOClients = make(map[string]*minio.Client) // key = host+key+secret
	mu           sync.Mutex
)

func NewOptions() *Options {
	return &Options{
		Options: buf.NewOptions(),
	}
}

type Options struct {
	*buf.Options
}

func newClient(StoreHost, accessKey, secretKey string) (client *minio.Client, err error) {
	mu.Lock()
	defer mu.Unlock()

	client, _ = minIOClients[StoreHost+accessKey+secretKey]
	if client == nil {
		client, err = minio.New(StoreHost, accessKey, secretKey, true)
		minIOClients[StoreHost+accessKey+secretKey] = client
	}
	return client, err
}

// parsePth will parse an s3 path of the form:
// "s3://{bucket}/{path/to/object.txt}
// and return the bucket and object path.
// If either bucket or object are empty then
// pth was not in the correct format for parsing or
// object and or bucket do not exist in pth.
func parsePth(p string) (scheme, bucket, path string) {
	u, _ := url.Parse(p)
	bucket = u.Host
	path = strings.TrimLeft(u.Path, "/")
	if strings.Contains(bucket, ":") {

		i := strings.Index(path, "/")
		if i == -1 { // no / found in path
			return u.Scheme, path, ""
		}
		bucket = path[:i]
		path = strings.TrimLeft(path[i:], "/")
	}
	return u.Scheme, bucket, path
}

// Stat a directory or file for additional information
func Stat(pth string, accessKey, secretKey string, host string) (stat.Stats, error) {
	client, err := newClient(accessKey, secretKey, host)
	if err != nil {
		return stat.Stats{}, errors.Wrap(err, "client init")
	}
	_, bucket, objPth := parsePth(pth)
	info, err := client.StatObject(bucket, objPth, minio.StatObjectOptions{})
	// check if directory
	if err != nil {
		donech := make(chan struct{})
		defer close(donech)
		count := 0
		for range client.ListObjects(bucket, objPth, false, donech) {
			if info.Err != nil {
				return stat.Stats{}, err
			}
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
