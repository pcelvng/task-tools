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
	"path"
	"path/filepath"
	"sync"

	"github.com/pcelvng/task-tools/file/stat"
)

type Options struct {
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
	var hshr hash.Hash          // hasher
	var buf io.ReadWriteCloser  // houses the underlying buffer, if used
	var w, wHshr io.WriteCloser // write closer - for writing (and closing if using gzip)

	// check perms - writing happens at the end
	pth, err := checkFile(pth)
	if err != nil {
		return nil, err
	}

	// choose buffer
	tmpPth := ""
	if opt.UseTmpFile {
		err := os.MkdirAll(opt.TmpDir, 0700)
		if err != nil {
			return nil, err
		}

		fBuf, err = ioutil.TempFile(opt.TmpDir, opt.TmpPrefix)
		if err != nil {
			return nil, err
		}
		buf = fBuf
		tmpPth = fBuf.Name() // store name for removal at the end
	} else {
		buf = bBuf
	}

	// md5 hasher
	hshr = md5.New()

	// writers
	w = buf
	wHshr = &nopClose{hshr}

	// compression
	if ext := filepath.Ext(pth); ext == ".gz" {
		// file writer
		w, err = gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}

		// hash writer
		// same compressed bytes are sent to the hasher
		wHshr, err = gzip.NewWriterLevel(hshr, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}
	}

	// stats
	sts := stat.New()
	sts.Path = pth

	// make writer
	return &Writer{
		pth:    pth,
		buf:    buf,
		w:      w,
		hshr:   hshr,
		wHshr:  wHshr,
		tmpPth: tmpPth,
		sts:    sts,
	}, nil
}

type Writer struct {
	pth     string             // absolute file path
	buf     io.ReadWriteCloser // buffer
	w       io.WriteCloser     // write closer for active writes
	wHshr   io.WriteCloser     // write closer for active hashing (close is needed to flush compression)
	hshr    hash.Hash          // hasher
	tmpPth  string             // tmp path (if used)
	sts     stat.Stat
	aborted bool
	closed  bool
	mu      sync.Mutex
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

// Abort will:
// - clear and close buffer
//
// Calling Close after Abort will do nothing.
// Writing after calling Abort has undefined behavior.
func (w *Writer) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// check if closed
	if w.closed {
		return nil
	}
	w.aborted = true

	// close writers
	w.w.Close()
	w.wHshr.Close()

	// close and clear buffer
	w.buf.Close()
	if w.tmpPth != "" {
		err := os.Remove(w.tmpPth)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close will:
// - calculate final checksum
// - copy (mv) buffer to pth file
// - clear and close buffer
// - report any errors
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
	w.wHshr.Close()

	// copy

	// rm tmp
	if w.tmpPth != "" {
		err := os.Remove(w.tmpPth)
		if err != nil {
			return err
		}
	}

	return nil
}

// copy will copy the contents of buf
// to the path indicated at pth.
//
// Returns num of bytes copied and error.
func (w *Writer) copy() (int64, error) {
	// mv if using tmp file buffer
	if w.tmpPth != "" {
		// rename will move via hard link if
		// on the same file system (same partition).
		// otherwise it will do a system copy.
		errMv := os.Rename(w.tmpPth, w.pth)
		fInfo, err := os.Stat(w.pth)

		if errMv != nil {
			return fInfo.Size(), errMv
		}
		return fInfo.Size(), err
	}

	// copy from mem buffer
	_, f, err := openF(w.pth, false)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return io.Copy(f, w.buf)
}

// openF will open the pth file (in append mode if append = true)
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
		return pth, nil, err
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
		return pth, nil, err
	}

	return pth, fPth, nil
}

// checkFile will check:
// - is not dir
// - can be opened
// - can be closed
//
// Will remove file if it didn't exist before.
//
// Returns the cleaned absolute path and any errors.
func checkFile(pth string) (string, error) {
	// check if exists, if dir
	var exists bool
	fInfo, err := os.Stat(pth)
	if err == nil {
		if fInfo.IsDir() {
			return pth, errors.New("file path is directory")
		}

		if fInfo.Size() > 0 {
			exists = true
		}
	}

	// open
	// append mode to not destroy existing contents
	pth, f, err := openF(pth, true)
	if err != nil {
		return pth, err
	}

	// close
	errC := f.Close()

	// remove
	if !exists {
		err = os.Remove(pth)
		if err != nil {
			return pth, err
		}
	}

	return pth, errC
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

type nopClose struct {
	io.Writer
}

func (wc *nopClose) Close() error {
	return nil
}
