package minio

import (
	"context"
	"mime"
	"path/filepath"
	"sync"
	"time"

	minio "github.com/minio/minio-go/v7"

	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/stat"
)

func NewWriter(pth string, mOpt Option, opt *buf.Options) (*Writer, error) {
	// s3 client:
	// using minio client library;
	// final writing doesn't happen until Close is called
	// but getting the client now does authentication
	// so we know early of authentication issues.
	client, err := newClient(mOpt)
	if err != nil {
		return nil, err
	}

	return newWriterFromClient(pth, client, opt)
}

func newWriterFromClient(pth string, s3Client *minio.Client, opt *buf.Options) (*Writer, error) {
	if opt == nil {
		opt = buf.NewOptions()
	}

	// stats
	sts := stat.Stats{Path: pth}

	// compression
	if ext := filepath.Ext(pth); ext == ".gz" {
		opt.Compress = true
	}

	// buffer
	bfr, err := buf.NewBuffer(opt)
	if err != nil {
		return nil, err
	}
	tmpPth := bfr.Stats().Path

	// s3 bucket, objPth
	_, bucket, objPth := parsePth(pth)

	return &Writer{
		client:     s3Client,
		bfr:        bfr,
		bucket:     bucket,
		objPth:     objPth,
		tmpPth:     tmpPth,
		sts:        sts.ToSafe(),
		keepFailed: opt.KeepFailed,
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
	client *minio.Client
	bfr    *buf.Buffer
	sts    *stat.Safe

	tmpPth string
	bucket string // destination s3 bucket
	objPth string // destination s3 object path

	done bool
	mu   sync.Mutex

	keepFailed bool // keep local copy of failed file if file buffer is used
}

func (w *Writer) Write(p []byte) (n int, err error) {
	return w.bfr.Write(p)
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	return w.bfr.WriteLine(ln)
}

func (w *Writer) Stats() stat.Stats {
	sts := w.bfr.Stats()
	sts.Path = w.sts.Path()

	return sts
}

// Abort will:
// - clear and close buffer
//
// Calling Close after Abort will do nothing.
// Writing after calling Abort has undefined behavior.
func (w *Writer) Abort() (err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.done {
		return nil
	}
	w.done = true
	return w.bfr.Abort()
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

	if w.done {
		return nil
	}
	w.done = true

	// close buffer to finalize writes
	// and copy contents to final
	// location.
	// sets checksum and size.
	w.bfr.Close()

	// do copy
	_, err := w.copy()
	if err != nil {
		if w.keepFailed {
			w.bfr.Reset()
		} else {
			w.bfr.Cleanup()
		}
		return err
	}

	// set created
	w.sts.SetCreated(time.Now())

	return w.bfr.Cleanup()
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

	// copy tmp file buffer
	if w.tmpPth != "" {
		info, err := w.client.FPutObject(
			context.Background(),
			w.bucket,
			w.objPth,
			w.tmpPth,
			opts,
		)
		return info.Size, err
	}

	// copy memory buffer
	info, err := w.client.PutObject(
		context.Background(),
		w.bucket,
		w.objPth,
		w.bfr,
		w.bfr.Stats().Size,
		opts,
	)
	return info.Size, err
}
