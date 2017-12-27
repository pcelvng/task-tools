package local

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"errors"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

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
func NewWriter(pth string, append, lazy bool, tmpDir, tmpPrefix string) (*Writer, error) {
	// open file
	pth, fPth, err := openF(pth, append)
	if err != nil {
		return nil, err
	}

	// compression
	var fComp *gzip.Writer
	if ext := filepath.Ext(pth); ext == ".gz" {
		fComp, err = gzip.NewWriterLevel(fPth, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}
	}

	// mem buffer (default)
	var buf *bytes.Buffer
	fMem := bufio.NewWriter(buf)

	// tmp file buffer
	var fTmp *os.File
	if tmpDir != "" || tmpPrefix != "" {
		if tmpPrefix == "" {
			tmpPrefix = "local"
		}
		fTmp, err = ioutil.TempFile(tmpDir, tmpPrefix)
		if err != nil {
			return nil, err
		}
	}

	// md5 writer
	fMd5 := md5.New()

	// multi writer - for a single write operation
	var fWrit io.Writer
	if lazy {
		fWrit = io.MultiWriter(fMem, fMd5)
	} else if fComp != nil {
		fWrit = io.MultiWriter(fComp, fMd5)
	} else {
		fWrit = io.MultiWriter(fPth, fMd5)
	}

	// stats
	sts := stat.New()
	sts.Path = pth

	fInfo, err := fPth.Stat()
	if err != nil {
		return nil, err
	}
	fStartSize := fInfo.Size()

	// make writer
	return &Writer{
		fPth:  fPth,
		fComp: fComp,
		fMem:  fMem,
		fTmp:  fTmp,
		fMd5:  fMd5,
		fWrit: fWrit,
		bBuf:  buf,

		append:     append,
		lazy:       lazy,
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

	// make pth absolute
	pth, err := filepath.Abs(pth)
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
	// writers
	fPth  *os.File       // final file
	fComp io.WriteCloser // compression writer
	fMem  *bufio.Writer  // memory buffer for lazy writing
	fTmp  *os.File       // temp file - if one exists
	fMd5  hash.Hash      // calculate md5 hash during the file write
	fWrit io.Writer      // multi-writer; writer used to make Write calls.
	bBuf  *bytes.Buffer  // underlying memory buffer.

	append     bool // indicate that file should be written to as append
	lazy       bool // indicate to write to memory first and then flush to file in one operation.
	sts        stat.Stat
	fStartSize int64 // starting size of the final destination file.
	fEndSize   int64 // final file size (will be smaller than bytes written if using compression.
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
	n, err := w.fWrit.Write(p)
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
	// close compression
	var compErr error
	if w.fComp != nil {
		compErr = w.fComp.Close()
		// can get EINVAL type error if Close is called twice.
		if compErr == syscall.EINVAL {
			compErr = nil
		}
	}

	// tmp file
	var tmpErr, tmpRmErr error
	if w.fTmp != nil {
		// close tmp
		tmpName := w.fTmp.Name()
		tmpErr = w.fTmp.Close()
		// can get EINVAL type error if Close is called twice.
		if tmpErr == syscall.EINVAL {
			tmpErr = nil
		}

		// remove tmp
		tmpRmErr = os.Remove(tmpName)
	}

	// reset mem buffer
	w.fMem.Flush()
	w.bBuf.Reset()

	// close final file
	fName := w.fPth.Name()
	fErr := w.fPth.Close()
	// can get EINVAL type error if Close is called twice.
	if fErr == syscall.EINVAL {
		fErr = nil
	}

	// truncate or rm destination
	var fRmErr, statErr error
	if w.append {
		// compare original byte size with current byte size
		// to determine if truncating is necessary.
		var fInfo os.FileInfo
		fInfo, statErr = os.Stat(fName)
		if fInfo.Size() > w.fStartSize {
			fRmErr = os.Truncate(fName, w.fStartSize)
		}
	} else {
		fRmErr = os.Remove(fName)
	}

	// return the first err found
	if statErr != nil {
		return statErr
	}
	if fRmErr != nil {
		return fRmErr
	}
	if fErr != nil {
		return fErr
	}
	if tmpRmErr != nil {
		return tmpRmErr
	}
	if tmpErr != nil {
		return tmpErr
	}

	return nil
}

func (w *Writer) Close() error {
	// sync and flush before the copy/mv/append

	// copy from buffer to final
	// - append copy if append == true
	// - copy from mem buf to final
	// - copy from tmp file to final
	// - calculate checksum for entire file for appended
	// - calculate checksum correctly when using compression

	// close compression
	var compErr error
	if w.fComp != nil {
		compErr = w.fComp.Close()
		// can get EINVAL type error if Close is called twice.
		if compErr == syscall.EINVAL {
			compErr = nil
		}
	}

	// tmp file
	var tmpErr, tmpRmErr error
	if w.fTmp != nil {
		// close tmp
		tmpName := w.fTmp.Name()
		tmpErr = w.fTmp.Close()
		// can get EINVAL type error if Close is called twice.
		if tmpErr == syscall.EINVAL {
			tmpErr = nil
		}

		// remove tmp
		tmpRmErr = os.Remove(tmpName)
	}

	// reset mem buffer
	w.fMem.Flush()
	w.bBuf.Reset()

	// close final file
	fName := w.fPth.Name()
	fErr := w.fPth.Close()
	// can get EINVAL type error if Close is called twice.
	if fErr == syscall.EINVAL {
		fErr = nil
	}

	// truncate or rm destination
	var fRmErr, statErr error
	if w.append {
		// compare original byte size with current byte size
		// to determine if truncating is necessary.
		var fInfo os.FileInfo
		fInfo, statErr = os.Stat(fName)
		if fInfo.Size() > w.fStartSize {
			fRmErr = os.Truncate(fName, w.fStartSize)
		}
	} else {
		fRmErr = os.Remove(fName)
	}

	// return the first err found
	if statErr != nil {
		return statErr
	}
	if fRmErr != nil {
		return fRmErr
	}
	if fErr != nil {
		return fErr
	}
	if tmpRmErr != nil {
		return tmpRmErr
	}
	if tmpErr != nil {
		return tmpErr
	}

	return nil
}
