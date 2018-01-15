package local

import (
	"compress/gzip"
	"crypto/md5"
	"errors"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/file/util"
)

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
		tmpPth, fBuf, err = util.OpenTmp(opt.TmpDir, opt.TmpPrefix)
		buf = fBuf
	} else {
		bBuf := util.NewCloseBuf() // memory buffer
		buf = bBuf
	}

	// md5 hasher
	hshr = md5.New()

	// hash write closer
	wHshr = util.NewNopWriteCloser(hshr)

	// buf and hasher go in the same writer
	// so that gzipping only needs to happen once.
	// both underlying writers will get the same bytes.
	writers := make([]io.WriteCloser, 2)
	writers[0], writers[1] = buf, wHshr
	w = util.NewMultiWriteCloser(writers)

	// compression
	if ext := filepath.Ext(pth); ext == ".gz" {
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
	buf    io.ReadWriteCloser // buffer
	w      io.WriteCloser     // write closer for active writes
	wHshr  io.WriteCloser     // write closer for active hashing (close is needed to flush compression)
	hshr   hash.Hash          // hasher
	tmpPth string             // tmp path (if used)
	sts    stat.Stat

	done bool
	mu   sync.Mutex
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	_, err = w.Write(append(ln, '\n'))
	if err == nil {
		w.sts.AddLine()
	}

	return err
}

func (w *Writer) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done == true {
		return 0, nil
	}

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
// Writing after Abort will not write and will
// not return a nil-error.
func (w *Writer) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.done {
		return nil
	}
	w.done = true

	// close writers
	w.w.Close()
	w.buf.Close()

	// close and clear underlying write buffer
	// may or may not be the same Close method as w
	// if using gzip. We want to make sure to close both
	// to make sure to flush and sync everything to disk.
	// underlying buffer may still need to be closed
	w.buf.Close()
	return util.RmTmp(w.tmpPth)
}

// Close will:
// - calculate final checksum
// - set file size
// - set file created date
// - copy (mv) buffer to pth file
// - clear and close buffer
// - report any errors
//
// Calling Abort after Close will do nothing.
// Writing after Close will not write and will
// not return a nil-error.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// check if aborted
	if w.done {
		return nil
	}
	w.done = true

	// close writers
	w.w.Close()
	w.buf.Close()

	// do copy
	w.copy()

	// set checksum, size
	w.sts.SetCheckSum(w.hshr)
	w.sts.SetSize(fileSize(w.sts.Path))
	w.sts.SetCreated(fileCreated(w.sts.Path))

	// underlying buffer may still need to be closed
	w.buf.Close()
	return util.RmTmp(w.tmpPth)
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
