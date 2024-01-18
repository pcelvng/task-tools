package minio

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pcelvng/task-tools/file/stat"
)

var (
	// domain of s3 compatible api
	S3Host = "s3.amazonaws.com"
	GSHost = "storage.googleapis.com"
	// map that maintains minIO clients
	// to prevent creating new clients with
	// every file for the same auth credentials
	minIOClients = make(map[string]*minio.Client) // key = host+key+secret
	mu           sync.Mutex
)

//func NewOptions() *Options {
//	return &Options{
//		Options: buf.NewOptions(),
//	}
//}

type Option struct {
	//	*buf.Options
	Host      string
	AccessKey string
	SecretKey string
	Secure    bool
}

func (o Option) key() string {
	if o.Secure {
		return o.Host + o.AccessKey + o.SecretKey
	}
	return o.Host + o.AccessKey + o.SecretKey + "un"
}

func newClient(opt Option) (client *minio.Client, err error) {
	mu.Lock()
	defer mu.Unlock()

	client, _ = minIOClients[opt.key()]
	if client == nil {
		client, err = minio.New(opt.Host, &minio.Options{
			Creds:  credentials.NewStaticV4(opt.AccessKey, opt.SecretKey, ""),
			Secure: opt.Secure,
		})
		minIOClients[opt.key()] = client
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
func Stat(pth string, Opt Option) (stat.Stats, error) {
	client, err := newClient(Opt)
	if err != nil {
		return stat.Stats{}, fmt.Errorf("client init %w", err)
	}
	_, bucket, objPth := parsePth(pth)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := client.StatObject(ctx, bucket, objPth, minio.StatObjectOptions{})
	// check if directory
	if err != nil {

		count := 0
		for info := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: false, Prefix: objPth}) {
			if info.Err != nil {
				return stat.Stats{}, err
			}
			count++
		}
		if count > 0 {
			return stat.Stats{
				Path:  pth,
				Files: int64(count),
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
		Created:  info.LastModified.UTC().Format(time.RFC3339),
		IsDir:    false,
	}, err
}
