package local

import (
	"os"
	"time"

	"github.com/pcelvng/task-tools/file/buf"
)

func NewOptions() *Options {
	return &Options{
		Options: buf.NewOptions(),
	}
}

type Options struct {
	*buf.Options
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
