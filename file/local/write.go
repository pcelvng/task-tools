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
	"strings"
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

	var hshr hash.Hash          // hasher
	var buf io.ReadWriteCloser  // buffer
	var w, wHshr io.WriteCloser // w=writer, wHshr=write hasher

	// check perms - writing happens at the end
	pth, err := checkFile(pth)
	if err != nil {
		return nil, err
	}

	// choose buffer
	tmpPth := ""
	if opt.UseTmpFile {
		var fBuf *os.File // tmp file buffer
		tmpPth, fBuf, err = openTmp(opt.TmpDir, opt.TmpPrefix)
		buf = fBuf
	} else {
		var bBuf *closeBuf // bytes memory buffer
		bBuf = &closeBuf{}
		buf = bBuf
	}

	// md5 hasher
	hshr = md5.New()

	// hash write closer
	wHshr = &nopClose{hshr}

	// buf and hasher go in the same writer
	// so that gzipping only needs to happen once.
	// both underlying writers will get the same bytes.
	writers := make([]io.WriteCloser, 2)
	writers[0], writers[1] = buf, wHshr
	w = &multiWriteCloser{writers}

	// compression
	if ext := filepath.Ext(pth); ext == ".gz" {
		// file writer
		w, _ = gzip.NewWriterLevel(w, gzip.BestSpeed)
	}

	// stats
	sts := stat.New()
	sts.Path = pth

	// make writer
	return &Writer{
		buf:    buf,
		w:      w,
		hshr:   hshr,
		wHshr:  wHshr,
		tmpPth: tmpPth,
		sts:    sts,
	}, nil
}

type Writer struct {
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

func (w *Writer) WriteLine(ln []byte) (err error) {
	_, err = w.Write(append(ln, '\n'))
	if err == nil {
		w.sts.AddLine()
	}

	return
}

func (w *Writer) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
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

	// close and clear underlying write buffer
	// may or may not be the same Close method as w
	// if using gzip. We want to make sure to close both
	// to make sure to flush and sync everything to disk.
	// underlying buffer may still need to be closed
	w.buf.Close()
	return rmTmp(w.tmpPth)
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

	// do copy
	w.copy()

	// set checksum, size
	w.sts.SetCheckSum(w.hshr)
	w.sts.SetSizeFromPath(w.sts.Path)

	// underlying buffer may still need to be closed
	w.buf.Close()
	return rmTmp(w.tmpPth)
}

func rmTmp(tmpPth string) error {
	if tmpPth == "" {
		return nil
	}

	return os.Remove(tmpPth)
}

// copy will copy the contents of buf
// to the path indicated at pth.
//
// Returns num of bytes copied and error.
func (w *Writer) copy() (n int64, err error) {
	isDev := strings.HasPrefix(w.sts.Path, "/dev/")

	// mv if using tmp file buffer.
	// can't use rename for dev files.
	if w.tmpPth != "" && !isDev {
		// rename will move via hard link if
		// on the same file system (same partition).
		// otherwise it will do a system copy.
		errMv := os.Rename(w.tmpPth, w.sts.Path)
		fInfo, err := os.Stat(w.sts.Path)

		// still attempt to get destination file size
		if fInfo != nil {
			n = fInfo.Size()
		}

		if errMv != nil {
			return n, errMv
		}
		return n, err
	}

	// the tmp file buffer is already closed because
	// we needed to make sure contents are flushed to disk.
	if w.tmpPth != "" {
		var err error
		w.buf, err = os.Open(w.tmpPth)
		if err != nil {
			return 0, err
		}
	}

	// copy from mem or tmp file buffer
	_, f, _ := openF(w.sts.Path, false)
	defer closeF(w.sts.Path, f)
	return io.Copy(f, w.buf)
}

// openF will open the pth file (in append mode if append = true)
func openF(pth string, append bool) (string, *os.File, error) {
	var f *os.File

	// make pth absolute
	absPth, _ := filepath.Abs(pth)

	// check for trailing '/'
	// otherwise the '/' will get removed when converted
	// to an absolute path and instead of a directory could get
	// created as a file. If the user appends a '/' then we are going
	// to assume the user meant a directory. We don't write to dirs.
	if len(pth) > 0 && (pth[len(pth)-1] == '/') {
		err := errors.New("references a directory")
		if absPth != "/" {
			absPth = absPth + "/"
		}
		return absPth, f, &os.PathError{"path", absPth, err}
	}

	// special writers
	switch absPth {
	case "/dev/stdout":
		f = os.Stdout
		return absPth, f, nil
	}

	// make dir(s) if not exists
	// NOTE: the writer does not clean up directories
	err := os.MkdirAll(path.Dir(absPth), 0700)
	if err != nil {
		return absPth, f, err
	}

	// calculate file flags
	fFlag := os.O_CREATE | os.O_WRONLY
	if append {
		fFlag = fFlag | os.O_APPEND
	} else {
		fFlag = fFlag | os.O_TRUNC
	}

	// open
	f, err = os.OpenFile(absPth, fFlag, 0644)
	if err != nil {
		return absPth, f, err
	}
	f.Write(nil)

	return absPth, f, nil
}

func closeF(pth string, f *os.File) error {
	if f == nil {
		return nil
	}

	// do not close special files
	// doing so can cause weird side effects if
	// other processes are using them.
	switch pth {
	case "/dev/null", "/dev/stdout", "/dev/stderr":
		return nil
	}

	// close
	return f.Close()
}

func openTmp(dir, prefix string) (absTmp string, f *os.File, err error) {
	// normalize dir path
	dir, _ = filepath.Abs(dir)

	err = os.MkdirAll(dir, 0700)
	if err != nil {
		return absTmp, f, err
	}

	f, err = ioutil.TempFile(dir, prefix)
	if f != nil {
		absTmp, _ = filepath.Abs(f.Name())
	}
	return absTmp, f, err
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
		if !fInfo.IsDir() {
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
	errC := closeF(pth, f)

	// remove
	if !exists {
		os.Remove(pth)
	}

	return pth, errC
}

// closeBuf provides Close method
// to make bytes.Buffer a ReadWriteCloser
type closeBuf struct {
	bytes.Buffer
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

type multiWriteCloser struct {
	writers []io.WriteCloser
}

func (mw *multiWriteCloser) Write(p []byte) (n int, err error) {
	for _, w := range mw.writers {
		n, err = w.Write(p)
		if err != nil {
			return
		}
		if n != len(p) {
			err = io.ErrShortWrite
			return
		}
	}
	return len(p), nil
}

func (mw *multiWriteCloser) Close() (err error) {
	for _, w := range mw.writers {
		wErr := w.Close()
		if wErr != nil {
			err = wErr
		}
	}
	return err
}
