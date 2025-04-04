package stat

import (
	"encoding/hex"
	"hash"
	"sync/atomic"
	"time"
)

type Safe struct {
	// LineCnt returns the file line count.
	LineCnt int64 `json:"linecnt,omitempty"`

	// ByteCount returns uncompressed raw file byte count.
	ByteCnt int64 `json:"bytecnt,omitempty"`

	// Size holds the actual file size.
	Size int64 `json:"size"`

	// Checksum returns the base64 encoded string of the file md5 hash.
	checksum atomic.Value `json:"-"`
	//Checksum string        `json:"checksum,omitempty"`

	// Path returns the full absolute path of the file.
	path atomic.Value `json:"-"`
	//Path string        `json:"path" uri:"origin"`

	// Created returns the date the file was created or last updated;
	// whichever is more recent.
	created atomic.Value `json:"-"`
	//Created string        `json:"created"`

	isDir atomic.Value `json:"-"`
	//IsDir bool          `json:"isDir,omitempty"`

	Files int64 `json:"files,omitempty"`
}

// AddLine will atomically and safely increment
// LineCnt by one.
func (s *Safe) AddLine() {
	atomic.AddInt64(&s.LineCnt, 1)
}

// AddBytes will atomically and safely increment
// ByteCnt by 'cnt'.
func (s *Safe) AddBytes(cnt int64) {
	atomic.AddInt64(&s.ByteCnt, cnt)
}

// SetChecksum will correctly calculate and set the
// base64 encoded checksum.
func (s *Safe) SetChecksum(hsh hash.Hash) {
	s.checksum.Store(hex.EncodeToString(hsh.Sum(nil)))
}

func (s *Safe) Checksum() string {
	return s.checksum.Load().(string)
}

func (s *Safe) SetSize(size int64) {
	curSize := atomic.LoadInt64(&s.Size)
	atomic.CompareAndSwapInt64(&s.Size, curSize, size)
}

func (s *Safe) SetPath(pth string) {
	s.path.Store(pth)
}

func (s *Safe) Path() string {
	return s.path.Load().(string)
}

// SetCreated will set the Created field in the
// format time.RFC3339 in UTC.
func (s *Safe) SetCreated(t time.Time) {
	s.created.Store(t.In(time.UTC).Format(time.RFC3339))
}

func (s *Safe) Created() string {
	return s.created.Load().(string)
}

func (s *Safe) SetDir(isDir bool) {
	s.isDir.Store(isDir)
}

func (s *Safe) IsDir() bool {
	return s.isDir.Load().(bool)
}

// ParseCreated will attempt to parse the Created
// field to a time.Time object.
// ParseCreated expects the Created time string is in
// time.RFC3339. If there is a parse error
// then the time.Time zero value is returned.
//
// The returned time will always be in UTC.
func (s *Safe) ParseCreated() time.Time {
	t, _ := time.Parse(time.RFC3339, s.created.Load().(string))
	return t.In(time.UTC)
}

func (s *Safe) JSONBytes() []byte {
	return s.Stats().JSONBytes()
}

func (s *Safe) JSONString() string {
	return string(s.JSONBytes())
}

func (s *Safe) Stats() Stats {
	return Stats{
		LineCnt:  atomic.LoadInt64(&s.LineCnt),
		ByteCnt:  atomic.LoadInt64(&s.ByteCnt),
		Size:     atomic.LoadInt64(&s.Size),
		Files:    atomic.LoadInt64(&s.Files),
		Checksum: s.checksum.Load().(string),
		Path:     s.path.Load().(string),
		Created:  s.created.Load().(string),
		IsDir:    s.isDir.Load().(bool),
	}
}
