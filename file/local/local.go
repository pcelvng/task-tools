package local

import (
	"os"
	"time"
)

func NewOptions() *Options {
	return &Options{}
}

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

// fileSize will return the file size from pth.
// If file could not be found at pth then returned
// size is 0.
func fileSize(pth string) int64 {
	fInfo, _ := os.Stat(pth)
	if fInfo != nil {
		return fInfo.Size()
	}
	return 0
}

// fileCreated will return the date the file
// was created.
//
// If file is not found at pth then a zero time.Time
// value is returned.
func fileCreated(pth string) time.Time {
	fInfo, _ := os.Stat(pth)
	if fInfo != nil {
		return fInfo.ModTime()
	}
	return time.Time{}
}
