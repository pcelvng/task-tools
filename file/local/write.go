package local

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pcelvng/task-tools/file/stat"
)

func NewWriter(pth, tmpDir string, append bool) (*Writer, error) {
	// temp file - only created if tmpDir is non-zero
	tmpPth := ""
	var err error
	var tmpF *os.File
	if tmpDir != "" {
		tmpPth, tmpF, err = openTmp(tmpDir)
		if err != nil {
			return nil, err
		}
	}

	// destination file
	var f *os.File
	pth, f, err = openF(pth, append)

	// make writer
	return &Writer{
		f:       f,
		tmpF:    tmpF,
		pth:     pth,
		tmpPth:  tmpPth,
		append:  append,
		fStat:   stat.NewStat(),
		tmpStat: stat.NewStat(),
	}, nil
}

// openTmp will always attempt to create and open temp file even
// if tmpDir is empty. An empty tmpDir will simply create the file
// in the current working directory.
func openTmp(tmpDir string) (string, *os.File, error) {
	// if tmpDir provided then create a tmp file in tmpDir
	tmpName := genTmpName()
	tmpPth, err := filepath.Abs(tmpDir + "/" + tmpName)
	if err != nil {
		return "", nil, err
	}

	tmpF, err := os.OpenFile(tmpPth, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		return "", nil, err
	}

	return tmpPth, tmpF, nil
}

func openF(pth string, append bool) (string, *os.File, error) {
	if pth == "" {
		return "", nil, errors.New("local file write path empty")
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
	f, err := os.OpenFile(pth, fFlag, 0644)
	if err != nil {
		return "", nil, err
	}

	return pth, f, nil
}

func genTmpName() string {
	seed := []byte(time.Now().Format(time.RFC3339Nano))

	hasher := sha1.New()
	hasher.Write(seed)
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

type Writer struct {
	f      *os.File
	tmpF   *os.File
	pth    string // absolute path
	tmpPth string // absolute temp file path
	append bool

	lineCnt int64
	hsh     hash.Hash64
	tmpStat *stat.Stat // tmp file stats
	fStat   *stat.Stat // final file stats
}

func (w *Writer) WriteLine([]byte) (int64, error) { return 0, nil }

func (w *Writer) Finish() error {

	// manage tmp file
	if w.tmpF != nil {
		err := w.tmpF.Sync()
		if err != nil {
			// there was a problem, rm the tmpFile
			w.rmTmp()
			return err
		}

		fInfo, err := w.tmpF.Stat()
		if err != nil {
			// there was a problem, rm the tmpFile
			w.rmTmp()
			return err
		}

		w.tmpStat.ByteCnt = fInfo.Size()
		err = w.tmpF.Close()
		if err != nil {
			// there was a problem, rm the tmpFile
			w.rmTmp()
			return err
		}

		written, err := w.mvTmp()
		if err != nil {
			// there was a problem, rm the tmpFile
			w.rmTmp()
			return err
		}
		w.fStat.ByteCnt = written
	} else {
		err := w.f.Sync()
		if err != nil {
			return err
		}

		err = w.f.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// mvTmp will attempt to copy the tmp file (if exists)
// to the final local destination and remove the tmp file.
//
// mvTmp will do nothing and return nil if the tmp file is
// not being used.
//
// If the tmp and final files are located in the same partition
// then a hard link will be created to the new location and the tmp
// location will be removed.
//
// If they are in different partitions then the tmp file is copied
// to the final location and when the copy is complete the tmp
// file is removed.
//
// If the destination file already exists then it will be over-written.
func (w *Writer) mvTmp() (int64, error) {
	// try hard link
	err := os.Link(w.tmpPth, w.pth)
	if err == nil {
		w.rmTmp()
		fInfo, _ := os.Stat(w.pth)
		return fInfo.Size(), nil
	}

	// try mv
	err = os.Rename(w.tmpPth, w.pth)
	if err == nil {
		fInfo, _ := os.Stat(w.pth)
		return fInfo.Size(), nil
	}

	// try cp
	written, err := io.Copy(w.f, w.tmpF)
	if err != nil {
		return written, err
	}
	err = w.rmTmp()
	if err != nil {
		return written, err
	}

	return written, nil
}

func (w *Writer) rmTmp() error {
	return os.Remove(w.tmpPth)
}

func (w *Writer) Stats() *stat.Stat {
	stats := stat.NewStat()
	stats.Path = w.pth
	return stats
}

func (w *Writer) Close() error { return nil }
