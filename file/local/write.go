package local

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/stat"
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
	// remove local:// prefix if exists
	pth = rmLocalPrefix(pth)

	if opt == nil {
		opt = NewOptions()
	}

	// stats
	sts := stat.New()
	pth, _ = filepath.Abs(pth)
	sts.SetPath(pth)

	// compression
	if ext := filepath.Ext(pth); ext == ".gz" {
		opt.Compress = true
	}

	// buffer
	bfr, err := buf.NewBuffer(opt.Options)
	if err != nil {
		return nil, err
	}
	tmpPth := bfr.Stats().Path

	// check file permissions
	pth, err = checkFile(pth)
	if err != nil {
		return nil, err
	}

	// make writer
	return &Writer{
		bfr:    bfr,
		sts:    sts,
		tmpPth: tmpPth,
	}, nil
}

type Writer struct {
	bfr    *buf.Buffer
	sts    stat.Stats
	tmpPth string
}

func (w *Writer) Write(p []byte) (n int, err error) {
	return w.bfr.Write(p)
}

func (w *Writer) WriteLine(ln []byte) (err error) {
	return w.bfr.WriteLine(ln)
}

func (w *Writer) Stats() stat.Stats {
	sts := w.bfr.Stats()
	sts.Path = w.sts.Path
	sts.Created = w.sts.Created

	return sts
}

// Abort will:
// - clear and close buffer
// - prevent further writing
func (w *Writer) Abort() error {
	// remove the destination path if
	// zero bytes.
	fInfo, _ := os.Lstat(w.Stats().Path)
	if fInfo != nil {
		if fInfo.Size() == 0 {
			os.Remove(w.Stats().Path)
		}
	}
	return w.bfr.Abort()
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
	// close buffer to finalize writes
	// and copy contents to final
	// location.
	w.bfr.Close()
	_, err := w.copyAndClean()
	if err != nil {
		return err
	}

	// set created date
	s, err := Stat(w.sts.Path)
	w.sts = s

	return err
}

// copy will copy the contents of buf
// to the path indicated at pth.
//
// Returns num of bytes copied and error.
func (w *Writer) copyAndClean() (n int64, err error) {
	isDev := strings.HasPrefix(w.sts.Path, "/dev/")

	// mv if using tmp file buffer.
	// can't use rename for dev files.
	// Note: tmp file buffer cleanup not necessary with
	// a mv operation.
	if w.tmpPth != "" && !isDev {
		// rename will move via hard link if
		// on the same file system (same partition).
		// otherwise it will do a system copy.
		errMv := os.Rename(w.tmpPth, w.sts.Path)
		fInfo, err := os.Stat(w.sts.Path)

		// destination file size
		if fInfo != nil {
			n = fInfo.Size()
		}

		if errMv != nil {
			// unable to mv so tmp file
			// cleanup still necessary.
			w.bfr.Cleanup()
			return n, errMv
		}
		return n, err
	}

	// byte by byte copy
	_, f, _ := openF(w.sts.Path, false)
	n, err = io.Copy(f, w.bfr)
	closeF(w.sts.Path, f) // assure disk sync
	w.bfr.Cleanup()

	return n, err
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
