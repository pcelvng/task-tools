package nop

import (
	"crypto/md5"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pcelvng/task-tools/file/stat"
)

// Stat can be used as a mock stat for testing.
// The following are supported mocks
// error, err, stat_error or stat_err - return an error when called
// stat_dir - identifies the path as a directory
func Stat(pth string) (stat.Stats, error) {
	u, _ := url.ParseRequestURI(pth)

	switch strings.ToLower(u.Host) {
	case "error", "err", "stat_error", "stat_err":
		return stat.Stats{}, errors.New("nop stat error")
	}
	return stat.Stats{
		Path:     pth,
		LineCnt:  10,
		Size:     123,
		Checksum: fmt.Sprintf("%x", md5.Sum([]byte(pth))),
		IsDir:    strings.Contains(strings.ToLower(pth), "stat_dir"),
		Created:  time.Now().UTC().Truncate(24 * time.Hour).Format(time.RFC3339),
	}, nil
}
