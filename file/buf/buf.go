package buf

import (
	"io"

	"bytes"
	"compress/gzip"
	"hash"
	"os"

	"crypto/md5"

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

	// size writer - so we can tell s3 what the
	// upload size is.
	wSize := util.NewSizeWriter()

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
		w:    w,
		hshr: hshr,
		sts:  sts,
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
	wGzip *gzip.Writer
	bBuf  bytes.Buffer // in-memory buffer
	fBuf  *os.File     // file buffer
	hshr  hash.Hash

	sts stat.Stat
}

func (b *Buffer) Read(p []byte) (n int, err error) {
	return
}

func (b *Buffer) WriteLine(ln []byte) (err error) {
	return
}

func (b *Buffer) Write(p []byte) (n int, err error) {
	return
}

func (b *Buffer) Stats() stat.Stat {
	return b.sts.Clone()
}

func (b *Buffer) Abort() error {
	return nil
}

// Cleanup will remove the tmp file (if exists)
// or reset the in-memory buffer (if used)
func (b *Buffer) Cleanup() error {
	return nil
}

func (w *Buffer) Close() error {
	return nil
}
