package stat

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"sync/atomic"
)

/*
func New() Safe {
	s := Safe{
		checksum: &atomic.Value{},
		path:     &atomic.Value{},
		created:  &atomic.Value{},
	}
	s.checksum.Store("")
	s.path.Store("")
	s.created.Store("")
	s.isDir.Store(false)
	return s
}

// NewFromBytes creates Stats from
// json bytes.
func NewFromBytes(b []byte) Safe {
	sts := New()
	json.Unmarshal(b, &sts)
	return sts
}

// NewFromInfo creates Stats from a
// uri formatted info string.
func NewFromInfo(info string) Safe {
	sts := New()
	uri.Unmarshal(info, &sts)

	return sts
}
*/

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

// CalcCheckSum creates a md5 hash based on the bytes passed in.
// This is a common method to get a checksum of a file.
func CalcCheckSum(b []byte) string {
	return hex.EncodeToString(md5.New().Sum(b))
}

/*
// Clone will create a copy of stat that won't trigger
// race conditions. Use Clone if you are updating and
// reading from stats at the same time. Read from the
// clone.
func (s *Safe) Clone() Safe {
	c := Safe{
		LineCnt: atomic.LoadInt64(&s.LineCnt),
		ByteCnt: atomic.LoadInt64(&s.ByteCnt),
		Size:    atomic.LoadInt64(&s.Size),
		Files:   atomic.LoadInt64(&s.Files),
	}
	c.checksum.Store(s.checksum.Load())
	c.path.Store(s.checksum.Load())
	c.created.Store(s.checksum.Load())
	c.isDir.Store(s.checksum.Load())
	return c
} */
