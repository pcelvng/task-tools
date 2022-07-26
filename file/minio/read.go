package minio

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/md5"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jbsmith7741/go-tools/appenderr"
	minio "github.com/minio/minio-go/v7"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/file/util"
)

func NewReader(pth string, opt Option) (*Reader, error) {
	// get s3 client
	s3Client, err := newClient(opt)
	if err != nil {
		return nil, err
	}

	return newReaderFromClient(pth, s3Client)
}

func newReaderFromClient(pth string, client *minio.Client) (*Reader, error) {
	sts := stat.New()
	sts.SetPath(pth)

	// get bucket, objPth and validate
	_, bucket, objPth := parsePth(pth)

	// get object
	obj, err := client.GetObject(context.Background(), bucket, objPth, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	// stats
	objInfo, err := obj.Stat()
	if err != nil {
		return nil, err
	}
	sts.SetCreated(objInfo.LastModified)
	sts.SetSize(objInfo.Size)

	// hash reader
	rHshr := util.NewHashReader(md5.New(), obj)

	// compression
	var rBuf *bufio.Reader
	var rGzip *gzip.Reader
	if ext := filepath.Ext(pth); ext == ".gz" {
		rGzip, err = gzip.NewReader(rHshr)
		if err != nil {
			return nil, err // problem reading header
		}
		rBuf = bufio.NewReader(rGzip)
	} else {
		rBuf = bufio.NewReader(rHshr)
	}

	return &Reader{
		obj:   obj,
		rBuf:  rBuf,
		rGzip: rGzip,
		rHshr: rHshr,
		sts:   sts,
	}, nil
}

// Reader will read in streamed bytes from the s3 object.NewS3Client
type Reader struct {
	obj   *minio.Object // s3 file object
	rBuf  *bufio.Reader
	rGzip *gzip.Reader
	rHshr *util.HashReader

	sts    stat.Stats
	closed bool
}

func (r *Reader) ReadLine() (ln []byte, err error) {
	ln, err = r.rBuf.ReadBytes('\n')

	if len(ln) > 0 {
		r.sts.AddLine()

		// note that even '\n' bytes are
		// accounted for.
		r.sts.AddBytes(int64(len(ln)))

		// drop newline characters
		if ln[len(ln)-1] == '\n' {
			drop := 1
			if len(ln) > 1 && ln[len(ln)-2] == '\r' { // windows newline
				drop = 2
			}
			ln = ln[:len(ln)-drop]
		}
	}
	return ln, err
}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.rBuf.Read(p)
	r.sts.AddBytes(int64(n))
	return n, err
}

func (r *Reader) Stats() stat.Stats {
	return r.sts.Clone()
}

func (r *Reader) Close() (err error) {
	if r.closed {
		return nil
	}

	if r.rGzip != nil {
		r.rGzip.Close()
	}
	err = r.obj.Close()

	// calculate checksum
	r.sts.SetChecksum(r.rHshr.Hshr)

	r.closed = true
	return err
}

// ListFiles will list all file objects in the provided pth directory.
// pth is assumed to be a directory and so a trailing "/" is appended
// if one does not already exist.
func ListFiles(pth string, opt Option) ([]stat.Stats, error) {
	// get client
	client, err := newClient(opt)
	if err != nil {
		return nil, err
	}
	isMinioHost := strings.Contains(pth, opt.Host)

	scheme, bucket, objPth := parsePth(pth)

	// objPth should always have trailing '/' (assumed to be dir)
	if !strings.HasSuffix(objPth, "/") {
		objPth = objPth + "/"
	}

	// create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{}) // being used like a context.Context

	// indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	allSts := make([]stat.Stats, 0)
	objInfoCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Prefix: objPth, Recursive: false})
	errs := appenderr.New()
	for objInfo := range objInfoCh {
		// don't include err objects
		if objInfo.Err != nil {
			errs.Add(objInfo.Err)
			continue
		}

		sts := stat.New()
		sts.IsDir = strings.HasSuffix(objInfo.Key, "/")
		sts.SetCreated(objInfo.LastModified)
		sts.Checksum = strings.Trim(objInfo.ETag, `"`) // returns checksum with '"'

		if isMinioHost {
			sts.SetPath(fmt.Sprintf("%s://%s/%s/%s", scheme, opt.Host, bucket, objInfo.Key))
		} else {
			sts.SetPath(fmt.Sprintf("%s://%s/%s", scheme, bucket, objInfo.Key))
		}
		sts.SetSize(objInfo.Size)

		allSts = append(allSts, sts)
	}

	return allSts, errs.ErrOrNil()
}
