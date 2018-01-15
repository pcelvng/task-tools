package buf

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"hash"
	"io"
	"os"

	"sync"

	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/file/util"
)

func NewOptions() *Options {
	return &Options{}
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
}

func NewBuffer(opt *Options) (b *Buffer, err error) {
	var bBuf *bytes.Buffer
	var fBuf *os.File
	var wGzip *gzip.Writer
	var w io.Writer

	if opt == nil {
		opt = NewOptions()
	}

	// stats
	sts := stat.New()

	// make tmp file
	if opt.UseFileBuf {
		sts.Path, fBuf, err = util.OpenTmp(opt.FileBufDir, opt.FileBufPrefix)
		if err != nil {
			return nil, err
		}
	}

	// hash write closer
	hshr := md5.New()

	// size writer - for knowing the number of bytes
	// written to the buffer.
	wSize := &sizeWriter{}

	// buf and hasher go in the same writer
	// so that gzipping only needs to happen once.
	// both underlying writers will get the same bytes.
	writers := make([]io.Writer, 3)
	if fBuf != nil {
		writers[0] = fBuf
	} else {
		writers[0] = bBuf
	}
	writers[1], writers[2] = hshr, wSize
	w = io.MultiWriter(writers...)

	// compression
	if opt.Compress {
		wGzip, _ = gzip.NewWriterLevel(w, gzip.BestSpeed)
		w = wGzip
	}

	// make writer
	return &Buffer{
		w:     w,
		wGzip: wGzip,
		bBuf:  bBuf,
		fBuf:  fBuf,
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
	bBuf  *bytes.Buffer // in-memory buffer
	fBuf  *os.File      // file buffer
	hshr  hash.Hash

	sts stat.Stat

	done bool // set to true if Close or Abort is called
	mu   sync.Mutex
}

func (bfr *Buffer) Read(p []byte) (n int, err error) {
	return
}

func (bfr *Buffer) WriteLine(ln []byte) (err error) {
	return
}

func (bfr *Buffer) Write(p []byte) (n int, err error) {
	bfr.mu.Lock()
	defer bfr.mu.Unlock()

	return
}

func (bfr *Buffer) Stats() stat.Stat {
	return bfr.sts.Clone()
}

// Abort will clear the buffer (remove tmp file if exists)
// and prevent further buffer writes.
func (bfr *Buffer) Abort() error {
	bfr.mu.Lock()
	defer bfr.mu.Unlock()

	if bfr.done {
		return nil
	}
	bfr.done = true

	// rm tmp file
	util.RmTmp(bfr.sts.Path)

	return nil
}

// Cleanup will remove the tmp file (if exists)
// or reset the in-memory buffer (if used)
func (bfr *Buffer) Cleanup() error {
	return nil
}

func (bfr *Buffer) Close() error {
	bfr.mu.Lock()
	defer bfr.mu.Unlock()

	if bfr.done {
		return nil
	}
	bfr.done = true

	return nil
}
