package local

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
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
		f:      f,
		tmpF:   tmpF,
		pth:    pth,
		tmpPth: tmpPth,
		append: append,
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
	tmpPth string // aboluste temp file path
	append bool
}

func (w *Writer) WriteLine([]byte) (int64, error) { return 0, nil }

func (w *Writer) Finish() error { return nil }

func (w *Writer) Stats() *stat.Stat {
	stats := stat.NewStat()
	stats.Path = w.pth
	return stats
}

func (w *Writer) Close() error { return nil }
