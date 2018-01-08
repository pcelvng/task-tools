package s3

import (
	"compress/gzip"
	"crypto/md5"
	"hash"
	"io"
	"mime"
	"os"
	"path/filepath"
	"sync"

	"github.com/minio/minio-go"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/file/util"
)

func NewWriter(pth string, accessKey, secretKey string, opt *Options) (*Writer, error) {
	if opt == nil {
		opt = new(Options)
	}

	// set bucket, objPth
	bucket, objPth, err := parsePth(pth)
	if err != nil {
		return nil, err
	}

	var hshr hash.Hash          // hasher
	var buf io.ReadWriteCloser  // buffer
	var w, wHshr io.WriteCloser // w=writer, wHshr=write hasher

	// s3 client - using minio client library
	// final writing doesn't happen until Close is called
	// but getting the client now does authentication
	// so that if auth or setup fails then we know early.
	s3Client, err := minio.New(
		"s3.amazonaws.com", accessKey, secretKey, true,
	)
	if err != nil {
		return nil, err
	}

	// choose buffer
	tmpPth := ""
	if opt.UseFileBuf {
		var fBuf *os.File // tmp file buffer
		tmpPth, fBuf, err = util.OpenTmp(opt.FileBufDir, opt.FileBufPrefix)
		buf = fBuf
	} else {
		bBuf := util.NewCloseBuf() // memory buffer
		buf = bBuf
	}

	// md5 hasher
	hshr = md5.New()

	// hash write closer
	wHshr = util.NewNopWriteCloser(hshr)

	// size writer - so we can tell s3 what the
	// upload size is.
	wSize := &sizeWriter{}

	// buf and hasher go in the same writer
	// so that gzipping only needs to happen once.
	// both underlying writers will get the same bytes.
	writers := make([]io.WriteCloser, 3)
	writers[0], writers[1], writers[2] = buf, wHshr, wSize
	w = util.NewMultiWriteCloser(writers)

	// compression
	if ext := filepath.Ext(pth); ext == ".gz" {
		w, _ = gzip.NewWriterLevel(w, gzip.BestSpeed)
	}

	// stats
	sts := stat.New()
	sts.SetPath(pth)

	return &Writer{
		s3Client: s3Client,
		buf:      buf,
		w:        w,
		hshr:     hshr,
		wHshr:    wHshr,
		wSize:    wSize,
		bucket:   bucket,
		objPth:   objPth,
		tmpPth:   tmpPth,
		sts:      sts,
	}, nil
}

// Writer will write to local buffer first
// and will copy all the written contents
// to the S3 destination after calling Close().
// Close() must be called in order for the written
// contents to be written to S3.
//
// Calling Abort() before Close() will cleanup the
// buffer. Calling Close() after Abort() will not
// result in any writing to S3.
//
// Calling Abort() after Close() will do nothing.
type Writer struct {
	s3Client *minio.Client
	buf      io.ReadWriteCloser // buffer
	w        io.WriteCloser     // write closer for active writes
	wHshr    io.WriteCloser     // write closer for active hashing (close is needed to flush compression)
	wSize    *sizeWriter        // to keep track of actual file size
	hshr     hash.Hash          // hasher
	bucket   string             // destination s3 bucket
	objPth   string             // destination s3 object path
	tmpPth   string             // tmp file buffer path
	sts      stat.Stat
	aborted  bool
	closed   bool
	mu       sync.Mutex
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	_, err = w.Write(append(ln, '\n'))
	if err == nil {
		w.sts.AddLine()
	}

	return err
}

func (w *Writer) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.sts.AddBytes(int64(n))
	if err != nil {
		return n, err
	}

	return n, err
}

func (w *Writer) Stats() stat.Stat {
	return w.sts.Clone()
}

// Abort will:
// - clear and close buffer
//
// Calling Close after Abort will do nothing.
// Writing after calling Abort has undefined behavior.
func (w *Writer) Abort() (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// check if closed
	if w.closed {
		return nil
	}
	w.aborted = true

	// close writers
	w.w.Close()
	w.buf.Close()

	return util.RmTmp(w.tmpPth)
}

// Close will:
// - calculate final checksum
// - copy (mv) buffer to pth file
// - clear and close buffer
// - report any errors
//
// If an error is returned it should be assumed
// that S3 object writing failed.
//
// Calling Abort after Close will do nothing.
// Writing after calling Close has undefined behavior.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// check if aborted
	if w.aborted {
		return nil
	}
	w.closed = true

	// close writers
	w.w.Close()
	w.buf.Close()

	// do copy
	n, err := w.copy()
	if err != nil {
		return err
	}

	// set checksum, size
	w.sts.SetCheckSum(w.hshr)
	w.sts.SetSize(n)

	return util.RmTmp(w.tmpPth)
}

// copy will copy the contents of buf
// to the s3 path indicated at bucket and
// objPth.
//
// Returns num of bytes copied and error.
func (w *Writer) copy() (n int64, err error) {
	// Set contentType based on filepath extension if not given or default
	// value of "application/octet-stream" if the extension has no associated type.
	opts := minio.PutObjectOptions{}
	if opts.ContentType = mime.TypeByExtension(filepath.Ext(w.objPth)); opts.ContentType == "" {
		opts.ContentType = "application/octet-stream"
	}

	// the tmp file buffer is already closed because
	// we needed to make sure contents are flushed to disk.
	if w.tmpPth != "" {
		var err error
		var f *os.File
		f, err = os.Open(w.tmpPth)
		if err != nil {
			return 0, err
		}
		w.buf = f
	}

	// write s3 object
	return w.s3Client.PutObject(
		w.bucket,
		w.objPth,
		w.buf,
		w.wSize.Size(),
		opts,
	)
}

// sizeWriter will perform a nop write and
// close. It will keep track of the total number
// of bytes written and provides a Size()
// method to know the total number of bytes written.
type sizeWriter struct {
	size int64
	mu   sync.Mutex
}

func (w *sizeWriter) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.size
}

func (w *sizeWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.size = w.size + int64(len(p))
	return len(p), nil
}

func (w *sizeWriter) Close() error {
	return nil
}
