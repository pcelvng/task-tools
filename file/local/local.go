package local

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pcelvng/task-tools/file/buf"
	"github.com/pcelvng/task-tools/file/stat"
)

func NewOptions() *Options {
	return &Options{
		Options: buf.NewOptions(),
	}
}

type Options struct {
	*buf.Options
}

/*
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
*/
func rmLocalPrefix(pth string) string {
	if strings.HasPrefix(pth, "local://./") {
		// replace for relative path
		pth = strings.Replace(pth, "local://./", "./", 1)
	} else if strings.HasPrefix(pth, "local:///") {
		// can specify "///" to indicate absolute path
		pth = strings.Replace(pth, "local:///", "/", 1)
	} else if strings.HasPrefix(pth, "local://") {
		// if not using "./" then pth is treated as abs path.
		pth = strings.Replace(pth, "local://", "/", 1)
	}
	return pth
}

// Stat returns a summary stats of a file or directory.
// It can be used to verify read permissions
func Stat(pth string) (stat.Stats, error) {
	p := rmLocalPrefix(pth)
	i, err := os.Stat(p)
	if err != nil {
		return stat.Stats{}, err
	}
	// md5 the file
	var checksum string
	if b, err := ioutil.ReadFile(pth); err == nil {
		checksum = fmt.Sprintf("%x", md5.Sum(b))
	}
	return stat.Stats{
		Checksum: checksum,
		Size:     i.Size(),
		Path:     pth,
		IsDir:    i.IsDir(),
		Created:  i.ModTime().String(),
	}, err
}

// ListFiles will list all files in the provided pth directory.
// pth must be a directory.
//
// Will not list recursively
// Checksums are not returned.
func ListFiles(pth string) ([]stat.Stats, error) {
	// remove local:// prefix if exists
	pth = rmLocalPrefix(pth)

	pth, _ = filepath.Abs(pth)
	filesInfo, err := ioutil.ReadDir(pth)
	if err != nil {
		return nil, err
	}

	allSts := make([]stat.Stats, 0)
	for _, fInfo := range filesInfo {
		sts := stat.New()
		sts.SetCreated(fInfo.ModTime())
		sts.SetPath(path.Join(pth, fInfo.Name())) // full abs path
		sts.SetSize(fInfo.Size())
		sts.IsDir = fInfo.IsDir()

		// md5 the file
		if !sts.IsDir {
			if b, err := ioutil.ReadFile(sts.Path); err == nil {
				sts.Checksum = fmt.Sprintf("%x", md5.Sum(b))
			}
		}
		allSts = append(allSts, sts)
	}

	return allSts, nil
}
