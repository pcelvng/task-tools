package local

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"errors"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"path"

	"github.com/pcelvng/task-tools/file/stat"
)

type Options struct {
	// Append will append to the file instead of truncating.
	Append bool

	// Delay will delay final writing until Close is called.
	// Will write to memory unless a TmpDir and/or TmpPrefix
	// is specified. In which case it will write to a temp
	// file until close is called.
	Delay bool

	// UseTmpFile specifies to use a tmp file for the delayed writing.
	// Can optionally also specify the tmp directory and tmp name
	// prefix.
	UseTmpFile bool

	// TmpDir optionally specifies the temp directory. If not specified then
	// the os default temp dir is used.
	TmpDir string

	// TmpPrefix optionally specifies the temp file prefix.
	// The full tmp file name is randomly generated and guaranteed
	// not to conflict with existing files. A prefix can help one find
	// the tmp file.
	TmpPrefix string
}

// NewWriter will create a new local writer.
// - 'pth' is the full path (with filename) that will be
// written. If the final file extension is a supported compression
// format then the file will be compressed in that format.
// - 'append' indicates whether the write session will append the
// contents to an existing file or truncate and then write. The
// defaut is false which will truncate an existing file and then write.
// - 'lazy' set to 'true' will tell the writer to do a lazy write. This
// means that all write calls will write to memory first and then write
// to 'pth' when writing is complete with a final call to 'Close'. If
// Close is never called then the file will not be written.
//
// When initializing a new writer, pth is checked for the correct write
// permissions. An error is returned if the writer will not have the
// correct permissions.
//
// For lazy writing, the writer supports writing to memory or a temp file.
// The writer will use the temp file option if tmpDir and/or tmpPrefix is
// provided. The writer will remove a temp file with a call to Close.
func NewWriter(pth string, opt *Options) (*Writer, error) {
	if opt == nil {
		opt = new(Options)
	}

	var bBuf *closeBuf          // bytes memory buffer
	var fBuf *os.File           // tmp file buffer
	var hshr *closeHasher       // hasher will close method
	var rBuf io.ReadCloser      // houses the underlying buffer, if used
	var w, wHshr io.WriteCloser // write closers, one for writing actual bytes, one for checksum

	// open file
	pth, f, err := openF(pth, opt.Append)
	if err != nil {
		return nil, err
	}

	// get starting file size
	fInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fStartSize := fInfo.Size() // if > 0 then appending and file existed before

	// choose underlying readwritecloser
	isDelayed := opt.Delay
	tmpName := ""
	if isDelayed {
		if opt.UseTmpFile {
			// attempt to mk temp dir if not exists
			err := os.MkdirAll(opt.TmpDir, 0700)
			if err != nil {
				return nil, err
			}
			fBuf, err = ioutil.TempFile(opt.TmpDir, opt.TmpPrefix)
			if err != nil {
				return nil, err
			}
			rBuf = fBuf
			w = fBuf
			tmpName = fBuf.Name()
		} else {
			rBuf = bBuf
			w = fBuf
		}
	} else {
		w = f
	}

	// md5 hasher
	hshr = &closeHasher{md5.New()}
	wHshr = hshr

	// compression
	if ext := filepath.Ext(pth); ext == ".gz" {
		// file writer
		w, err = gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}

		// hash writer
		// this way the same compressed bytes are sent to the hasher
		wHshr, err = gzip.NewWriterLevel(hshr, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}
	}

	// if appending to a file that already existed then turn off line-by-line hashing.
	if fStartSize > 0 {
		wHshr = &NopWriteCloser{}
	}

	// stats
	sts := stat.New()
	sts.Path = pth

	// make writer
	return &Writer{
		f:          f,
		rBuf:       rBuf,
		w:          w,
		wHshr:      wHshr,
		hshr:       hshr,
		isDelayed:  isDelayed,
		tmpName:    tmpName,
		sts:        sts,
		fStartSize: fStartSize,
	}, nil
}

// openF will open the pth file.
//
// openF will examine the pth extension. If '.gz'
// is the final extension then the file writer
// will be wrapped with a gzip writer.
//
// the return pth path value will be a normalized absolute
// path.
//
// NOTE: if append != true then even if the file
// is being lazily written then the file will be truncated
// the moment it is opened.
func openF(pth string, append bool) (string, *os.File, error) {
	if pth == "" {
		return "", nil, errors.New("local file path empty")
	}

	// make dir if not exists
	err := os.MkdirAll(path.Dir(pth), 0700)
	if err != nil {
		return pth, nil, err
	}

	// make pth absolute
	pth, err = filepath.Abs(pth)
	if err != nil {
		return "", nil, err
	}

	// calculate file flags
	fFlag := os.O_CREATE | os.O_WRONLY
	if append {
		fFlag = fFlag | os.O_APPEND
	} else {
		fFlag = fFlag | os.O_TRUNC
	}

	// open
	fPth, err := os.OpenFile(pth, fFlag, 0644)
	if err != nil {
		return "", nil, err
	}

	return pth, fPth, nil
}

type Writer struct {
	f         *os.File       // file
	rBuf      io.ReadCloser  // tmp file read buffer
	w         io.WriteCloser // writer - calling Write will write to this writer
	wHshr     io.WriteCloser // writer for writing to the hasher - separate so that gzip can write to it.
	hshr      hash.Hash      // hasher
	isDelayed bool           // indicates if writing to final file is delayed until Close is called
	tmpName   string         // full path name of tmp file (if used)

	sts        stat.Stat
	fStartSize int64 // starting size of the final destination file. If > 0 then the file is being appended to.
}

func (w *Writer) WriteLine(ln []byte) (int64, error) {
	n, err := w.Write(append(ln, '\n'))
	if err != nil {
		return int64(n), err
	}

	// increment line count
	w.sts.AddLine()

	return int64(n), nil
}

func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if err != nil {
		return n, err
	}
	_, err = w.wHshr.Write(p)
	if err != nil {
		return n, err
	}
	written := int64(n)

	// increment byte count
	w.sts.AddBytes(written)

	return n, err
}

func (w *Writer) Stats() stat.Stat {
	return w.sts.Clone()
}

// Abort will close files and remove written bytes.
// - close and remove tmp file (if exists)
// - close truncate or remove final file
func (w *Writer) Abort() error {

	return nil
}

func (w *Writer) Close() error {
	// calculate entire checksum for final file at end
	// if file is appended.

	return nil
}

// closeBuf provides Close method
// to make bytes.Buffer a ReadWriteCloser
type closeBuf struct {
	*bytes.Buffer
}

func (b closeBuf) Close() error {
	// if the buffer has been read until EOF
	// then this is not necessary since Reset
	// is called internally when it reaches
	// EOF. However, if the the writer is aborted
	// then close will cleanup the buffer.
	b.Reset()
	return nil
}

type closeHasher struct {
	hash.Hash
}

func (h *closeHasher) Close() error {
	return nil
}

type NopWriteCloser struct{}

func (wc *NopWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (wc *NopWriteCloser) Close() error {
	return nil
}
