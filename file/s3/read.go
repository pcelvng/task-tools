package s3

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"path/filepath"

	"github.com/minio/minio-go"

	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/file/util"
)

func NewReader(pth string, accessKey, secretKey string) (*Reader, error) {
	sts := stat.New()
	sts.SetPath(pth)

	// get bucket, objPth and validate
	bucket, objPth := parsePth(pth)

	// s3 client - using minio client library
	s3Client, err := minio.New(storeEndpoint, accessKey, secretKey, true)
	if err != nil {
		return nil, err
	}

	// get object
	s3Obj, err := s3Client.GetObject(bucket, objPth, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	// stats
	objInfo, err := s3Obj.Stat()
	if err != nil {
		return nil, err
	}
	sts.SetCreated(objInfo.LastModified)
	sts.SetSize(objInfo.Size)

	// hash reader
	rHshr := util.NewHashReader(md5.New(), s3Obj)

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
		s3Obj: s3Obj,
		rBuf:  rBuf,
		rGzip: rGzip,
		rHshr: rHshr,
		sts:   sts,
	}, nil
}

// Reader will read in streamed bytes from the s3 object.
type Reader struct {
	s3Obj *minio.Object // s3 file object
	rBuf  *bufio.Reader
	rGzip *gzip.Reader
	rHshr *util.HashReader

	sts    stat.Stat
	closed bool
}

func (r *Reader) ReadLine() (ln []byte, err error) {
	ln, err = r.rBuf.ReadBytes('\n')

	if len(ln) > 0 {
		r.sts.AddLine()

		// note that even '\n' bytes are
		// accounted for.
		r.sts.AddBytes(int64(len(ln)))

		if ln[len(ln)-1] == '\n' {
			return ln[:len(ln)-1], err
		}
	}
	return ln, err
}

func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.rBuf.Read(p)
	r.sts.AddBytes(int64(n))
	return n, err
}

func (r *Reader) Stats() stat.Stat {
	return r.sts.Clone()
}

func (r *Reader) Close() (err error) {
	if r.closed {
		return nil
	}

	if r.rGzip != nil {
		r.rGzip.Close()
	}
	err = r.s3Obj.Close()

	// calculate checksum
	r.sts.SetCheckSum(r.rHshr.Hshr)

	r.closed = true
	return err
}
