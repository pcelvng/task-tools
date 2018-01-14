package stat

import (
	"encoding/hex"
	"encoding/json"
	"hash"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func New() Stat {
	return Stat{}
}

// NewFromBytes creates Stat from
// json bytes.
func NewFromBytes(b []byte) Stat {
	sts := New()
	json.Unmarshal(b, sts)
	return sts
}

type Stat struct {
	// LineCnt returns the file line count.
	LineCnt int64 `json: "linecnt"`

	// ByteCount returns uncompressed raw file byte count.
	ByteCnt int64 `json: "bytecnt"`

	// Size holds the actual file size.
	Size int64 `json: "size"`

	// Checksum returns the base64 encoded string of the file md5 hash.
	CheckSum string `json: "checksum"`

	// Path returns the full absolute path of the file.
	Path string `json: "path"`

	// Created returns the date the file was created or last updated;
	// whichever is more recent.
	Created string `json: "created"`

	mu sync.Mutex
}

// AddLine will atomically and safely increment
// LineCnt by one.
func (s *Stat) AddLine() {
	atomic.AddInt64(&s.LineCnt, 1)
}

// AddBytes will atomically and safely increment
// ByteCnt by 'cnt'.
func (s *Stat) AddBytes(cnt int64) {
	atomic.AddInt64(&s.ByteCnt, cnt)
}

// SetCheckSum will correctly calculate and set the
// base64 encoded checksum.
func (s *Stat) SetCheckSum(hsh hash.Hash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CheckSum = hex.EncodeToString(hsh.Sum(nil))
}

func (s *Stat) SetSizeFromPath(pth string) {
	fInfo, _ := os.Stat(pth)
	if fInfo != nil {
		curSize := atomic.LoadInt64(&s.Size)
		fSize := fInfo.Size()
		atomic.CompareAndSwapInt64(&s.Size, curSize, fSize)
	}
}

func (s *Stat) SetSize(size int64) {
	curSize := atomic.LoadInt64(&s.Size)
	atomic.CompareAndSwapInt64(&s.Size, curSize, size)
}

func (s *Stat) SetPath(pth string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Path = pth
}

// SetCreated will set the Created field in the
// format time.RFC3339.
func (s *Stat) SetCreated(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Created = t.Format(time.RFC3339)
}

// SetCreatedFromPath will set the Created field in the
// format time.RFC3339 from a local file path.
// If unsuccessful then the Created field will not
// be set. If it was previously set, the value will
// not change.
//
// Will pick the most recent date between the file
// creation and file updated date in case the file
// already existed but was replaced with new contents.
func (s *Stat) SetCreatedFromPath(pth string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fInfo, _ := os.Stat(pth)
	if fInfo != nil {
		t := fInfo.ModTime()
		s.Created = t.Format(time.RFC3339)
	}
}

// ParseCreated will attempt to parse the Created
// field to a time.Time object.
// ParseCreated expects the Created time string is in
// time.RFC3339. If there is a parse error
// then the time.Time zero value is returned.
func (s *Stat) ParseCreated() time.Time {
	t, _ := time.Parse(time.RFC3339, s.Created)
	return t
}

func (s *Stat) JSONBytes() []byte {
	b, _ := json.Marshal(s)
	return b
}

func (s *Stat) JSONString() string {
	return string(s.JSONBytes())
}

// Clone will create a copy of stat that won't trigger
// race conditions. Use Clone if you are updating and
// reading from stats at the same time. Read from the
// clone.
func (s *Stat) Clone() Stat {
	clone := New()

	s.mu.Lock()
	clone.CheckSum = s.CheckSum
	clone.Path = s.Path
	s.mu.Unlock()

	clone.LineCnt = atomic.LoadInt64(&s.LineCnt)
	clone.ByteCnt = atomic.LoadInt64(&s.ByteCnt)
	clone.Size = atomic.LoadInt64(&s.Size)
	return clone
}
