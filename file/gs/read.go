package gs

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/jbsmith7741/go-tools/appenderr"
	minio "github.com/minio/minio-go"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/file/util"
)

func NewReader(pth string, accessKey, secretKey string) (*Reader, error) {
	// get gs client
	gsClient, err := newGSClient(accessKey, secretKey)
	if err != nil {
		return nil, err
	}

	return newReaderFromGSClient(pth, gsClient)
}

func newReaderFromGSClient(pth string, gsClient *minio.Client) (*Reader, error) {
	sts := stat.New()
	sts.SetPath(pth)

	// get bucket, objPth and validate
	bucket, objPth := parsePth(pth)

	// get object
	opts := &minio.GetObjectOptions{}
	opts.Set("Accept-Encoding", "gzip") // needed to read file with the metadata gzip
	gsObj, err := gsClient.GetObject(bucket, objPth, *opts)
	if err != nil {
		return nil, err
	}

	// stats
	objInfo, err := gsObj.Stat()
	if err != nil {
		return nil, err
	}
	sts.SetCreated(objInfo.LastModified)
	sts.SetSize(objInfo.Size)

	// hash reader
	rHshr := util.NewHashReader(md5.New(), gsObj)

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
		gsObj: gsObj,
		rBuf:  rBuf,
		rGzip: rGzip,
		rHshr: rHshr,
		sts:   sts,
	}, nil
}

// Reader will read in streamed bytes from the gs object.NewgsClient
type Reader struct {
	gsObj *minio.Object // gs file object
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
	err = r.gsObj.Close()

	// calculate checksum
	r.sts.SetChecksum(r.rHshr.Hshr)

	r.closed = true
	return err
}

// ListFiles will list all file objects in the provided pth directory.
// pth is assumed to be a directory and so a trailing "/" is appended
// if one does not already exist.
func ListFiles(pth string, accessKey, secretKey string) ([]stat.Stats, error) {

	// get gs client
	gsClient, err := newGSClient(accessKey, secretKey)
	if err != nil {
		return nil, err
	}

	bucket, objPth := parsePth(pth)

	// objPth should always have trailing '/' (assumed to be dir)
	if !strings.HasSuffix(objPth, "/") {
		objPth = objPth + "/"
	}

	// create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{}) // being used like a context.Context

	// indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	allSts := make([]stat.Stats, 0)
	// ListObjectsV2 is not implemented for GS S3 API as of 7/8/2019
	// https://stackoverflow.com/questions/45638871/is-the-listobjectsv2-api-call-implemented-in-google-cloud-storage
	objInfoCh := gsClient.ListObjects(bucket, objPth, false, doneCh)
	errs := appenderr.New()
	for objInfo := range objInfoCh {

		// don't include dir and err objects
		if objInfo.Err != nil {
			errs.Add(objInfo.Err)
			continue
		}

		// object is referring to itself
		if objPth == objInfo.Key {
			continue
		}

		sts := stat.New()
		sts.IsDir = strings.HasSuffix(objInfo.Key, "/")
		sts.SetCreated(objInfo.LastModified)
		sts.Checksum = strings.Trim(objInfo.ETag, `"`) // returns checksum with '"'
		sts.SetPath(fmt.Sprintf("gs://%s/%s", bucket, objInfo.Key))
		sts.SetSize(objInfo.Size)

		allSts = append(allSts, sts)
	}

	return allSts, errs.ErrOrNil()
}
