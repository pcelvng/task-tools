package stat

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/jbsmith7741/uri"
)

// NewFromBytes creates Stats from
// json bytes.
func NewFromBytes(b []byte) (sts Stats) {
	json.Unmarshal(b, &sts)
	return sts
}

// NewFromInfo creates Stats from a
// uri formatted info string.
func NewFromInfo(info string) (sts Stats) {
	uri.Unmarshal(info, &sts)
	return sts
}

type Stats struct {
	LineCnt int64 `json:"linecnt,omitempty"`

	// ByteCount returns uncompressed raw file byte count.
	ByteCnt int64 `json:"bytecnt,omitempty"`

	// Size holds the actual file size.
	Size int64 `json:"size"`

	// Checksum base64 encoded string of the file md5 hash
	Checksum string `json:"checksum,omitempty"`

	// Path returns the full absolute path of the file.
	Path string `json:"path" uri:"origin"`

	// Created date the file was created or last updated Format(time.RFC3339)
	// whichever is more recent.
	Created string `json:"created"`

	IsDir bool `json:"isDir,omitempty"`

	Files int64 `json:"files,omitempty"`
}

func (s Stats) ToSafe() *Safe {
	c := &Safe{
		LineCnt: atomic.LoadInt64(&s.LineCnt),
		ByteCnt: atomic.LoadInt64(&s.ByteCnt),
		Size:    atomic.LoadInt64(&s.Size),
		Files:   atomic.LoadInt64(&s.Files),
	}
	c.checksum.Store(s.Checksum)
	c.path.Store(s.Path)
	c.created.Store(s.Created)
	c.isDir.Store(s.IsDir)
	return c
}

func (s Stats) JSONBytes() []byte {
	b, _ := json.Marshal(s)
	return b
}

func (s Stats) JSONString() string {
	return string(s.JSONBytes())
}

// Deprecated: reference Stats directly stat.Stats{}
func New() Stats {
	return Stats{}
}

// ParseCreated converts the store string timestamp to a time.Time value
func (s Stats) ParseCreated() time.Time {
	t, _ := time.Parse(time.RFC3339, s.Created)
	return t
}

// CalcCheckSum creates a md5 hash based on the bytes passed in.
// This is a common method to get a checksum of a file.
func CalcCheckSum(b []byte) string {
	return hex.EncodeToString(md5.New().Sum(b))
}
