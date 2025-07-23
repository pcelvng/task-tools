package local

import (
	"crypto/md5"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/pcelvng/task-tools/file/stat"
)

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
	if b, err := os.ReadFile(pth); err == nil {
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
	dirInfo, err := os.ReadDir(pth)
	if err != nil {
		return nil, err
	}

	allSts := make([]stat.Stats, 0)
	for _, d := range dirInfo {
		fInfo, err := d.Info()
		var sts stat.Stats
		if err != nil {
			// put error message in checksum
			sts.Checksum = err.Error()
			allSts = append(allSts, sts)
			continue
		}
		sts = stat.Stats{
			Created: fInfo.ModTime().Format(time.RFC3339),
			Path:    path.Join(pth, fInfo.Name()),
			Size:    fInfo.Size(),
			IsDir:   fInfo.IsDir(),
		}

		// md5 the file
		if !sts.IsDir {
			if b, err := os.ReadFile(sts.Path); err == nil {
				sts.Checksum = stat.CalcCheckSum(b)
			}
		}
		allSts = append(allSts, sts)
	}

	return allSts, nil
}
