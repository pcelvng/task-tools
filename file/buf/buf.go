package buf

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/file/util"
)

func NewOptions() *Options {
	return &Options{
		CompressType: gzip.BestSpeed,
	}
}

type Options struct {
	// UseFileBuf specifies to use a tmp file for the delayed writing.
	// Can optionally also specify the tmp directory and tmp name
	// prefix.
	UseFileBuf bool

	// FileBufDir optionally specifies the temp directory. If not specified then
	// the os default temp dir is used.
	FileBufDir string

	// FileBufPrefix optionally specifies the temp file prefix.
	// The full tmp file name is randomly generated and guaranteed
	// not to conflict with existing files. A prefix can help one find
	// the tmp file.
	FileBufPrefix string

	// Compress set to true will turn on gzip compression.
	// Writes will be compressed but reads will read raw
	// compressed bytes.
	Compress bool

	// CompressType type of compression used on the file
	// best speed, best compression, default
	CompressType int

	// KeepFailed files when using a file buffer and the
	// copy commands fails
	KeepFailed bool
}

func NewBuffer(opt *Options) (b *Buffer, err error) {
	var bBuf *bytes.Buffer
	var fBuf *os.File
	var wGzip *gzip.Writer
	var w io.Writer // write to buffer
	var r io.Reader // read from buffer

	if opt == nil {
		opt = NewOptions()
	}

	// stats
	sts := stat.New()
	sts.SetCreated(time.Now())

	// tmp file
	if opt.UseFileBuf {
		sts.Path, fBuf, err = util.OpenTmp(opt.FileBufDir, opt.FileBufPrefix)
		if err != nil {
			return nil, err
		}

		// open tmp file reader
		r, _ = os.Open(sts.Path)
	}

	// hash write closer
	hshr := md5.New()

	// size writer - keeps track of actual bytes written
	// so that when done writing the final file size is
	// known.
	wSize := &sizeWriter{}

	// buf and hasher go in the same writer
	// so that gzipping only needs to happen once.
	// both underlying writers will get the same bytes.
	writers := make([]io.Writer, 3)
	if fBuf != nil {
		writers[0] = fBuf
	} else {
		bBuf = &bytes.Buffer{}
		r = bBuf
		writers[0] = bBuf
	}
	writers[1], writers[2] = hshr, wSize
	w = io.MultiWriter(writers...)

	// compression
	if opt.Compress {
		wGzip, _ = gzip.NewWriterLevel(w, opt.CompressType)
		w = wGzip
	}

	// make writer
	return &Buffer{
		w:     w,
		wGzip: wGzip,
		wSize: wSize,
		bBuf:  bBuf,
		fBuf:  fBuf,
		r:     r,
		hshr:  hshr,
		sts:   sts,
	}, nil
}

// Buffer implements both StatsReadCloser and
// StatsWriteCloser interfaces.
//
// Buffer is meant to abstract away the details
// of writing and reading to either a file buffer
// or in-memory buffer.
//
// Buffer will:
// - compress writes if Options.Compress is true.
// - keep track of buffer statistics
// - calculate MD5 checksum on calling Close()
// - calculate the buffer size
// - provide the tmp file path in the file stats.
// - clean up tmp file if Abort() or Cleanup() are called.
type Buffer struct {
	w     io.Writer
	wGzip *gzip.Writer  // gzip writer (only if compression is enabled)
	wSize *sizeWriter   // keep of size of buffer
	bBuf  *bytes.Buffer // in-memory buffer (writing and reading)
	fBuf  *os.File      // file buffer (for writing)
	r     io.Reader     // underlying buffer (for reading)
	hshr  hash.Hash

	sts stat.Stats
	mu  sync.Mutex // safe concurrent writing

}

// Read will read the raw underlying buffer bytes.
// If the buffer is writing with compression it will
// not decompress on reads. Read is made for reading
// the final written bytes and copying them to the final
// location.
//
// Close should be called before Read as Close will
// sync the underlying buffer. This is especially
// important when using compression and/or a tmp file.
func (bfr *Buffer) Read(p []byte) (n int, err error) {
	return bfr.r.Read(p)
}

// Write will write to the underlying buffer. The underlying
// bytes writing will be compressed if compression was
// specified on buffer initialization.
//
// Write is thread safe.
func (bfr *Buffer) Write(p []byte) (n int, err error) {
	bfr.mu.Lock()
	defer bfr.mu.Unlock()

	// will write to:
	// - gzipper (if compression == true)
	// - underlying buffer
	// - hasher (for calculating final checksum)
	// - size tabulator (for knowing the total underlying byte size)
	n, err = bfr.w.Write(p)

	bfr.sts.AddBytes(int64(n))
	return n, err
}

// WriteLine will write to the underlying buffer. The underlying
// bytes writing will be compressed if compression was
// specified on buffer initialization.
//
// WriteLine is thread safe.
func (bfr *Buffer) WriteLine(ln []byte) (err error) {
	var n int
	n, err = bfr.Write(append(ln, '\n'))
	wantN := len(ln) + 1
	if err == nil && n == wantN {
		bfr.sts.AddLine()
	}

	return err
}

func (bfr *Buffer) Stats() stat.Stats {
	return bfr.sts.Clone()
}

// Abort will clear the buffer (remove tmp file if exists)
// and prevent further buffer writes.
func (bfr *Buffer) Abort() (err error) {
	// flush gzip writer (if exists)
	if bfr.wGzip != nil {
		bfr.wGzip.Close()
	}

	// cleanup underlying buffer
	err = bfr.Cleanup()

	return err
}

// Cleanup will remove the tmp file (if exists)
// or reset the in-memory buffer (if used).
//
// Cleanup should not be used until the user
// is done with the contents of the buffer.
//
// Cleanup is called automatically as part of the
// abort process but since the user may wish to
// read from the buffer after closing, Cleanup
// will need to be called after Close, especially
// if using compression since Close flushes the
// compression buffer and finalizes writing.
func (bfr *Buffer) Cleanup() (err error) {
	// cleanup bytes buffer (if used)
	if bfr.bBuf != nil {
		// reset still retains underlying slice
		bfr.bBuf.Reset()

		// replace current bytes buffer. The
		// gc will take care of clearing it
		// out completely.
		bfr.bBuf = &bytes.Buffer{}
	}

	// cleanup file buffer (if used)
	if bfr.fBuf != nil {
		// rm tmp file
		err = util.RmTmp(bfr.sts.Path)
	}
	return err
}

// Reset will reset the in-memory buffer (if used)
// and remove the reference to the tmp file (if exists)
//
// Reset does not verify that the tmp file is closed
func (bfr *Buffer) Reset() {
	// cleanup bytes buffer (if used)
	if bfr.bBuf != nil {
		// reset still retains underlying slice
		bfr.bBuf.Reset()

		// replace current bytes buffer. The
		// gc will take care of clearing it
		// out completely.
		bfr.bBuf = &bytes.Buffer{}
	}
	bfr.fBuf = nil
}

// Close prevents further writing
// and flushes writes to the underlying
// buffer.
func (bfr *Buffer) Close() (err error) {
	// flush gzip writer (if exists)
	if bfr.wGzip != nil {
		// if zero bytes written then add an empty line
		// so that a header is created. This behavior
		// matches gzipping a blank file from the command
		// line.
		if bfr.Stats().ByteCnt == 0 {
			bfr.Write([]byte("\n"))
		}
		err = bfr.wGzip.Close()
	}

	// close tmp file to sync writes
	// to disk.
	if bfr.fBuf != nil {
		err = bfr.fBuf.Close()
	}

	// set checksum, size
	bfr.sts.SetChecksum(bfr.hshr)
	bfr.sts.SetSize(bfr.wSize.Size())

	return err
}
