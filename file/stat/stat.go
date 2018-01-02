package stat

import (
	"encoding/hex"
	"hash"
	"os"
	"sync"
	"sync/atomic"
)

func New() Stat {
	return Stat{}
}

type Stat struct {
	// LineCnt returns the file line count.
	LineCnt int64

	// ByteCount returns uncompressed raw file byte count.
	ByteCnt int64

	// Size holds the actual file size.
	Size int64

	// Checksum returns the base64 encoded string of the file md5 hash.
	CheckSum string

	// Path returns the full absolute path of the file.
	Path string

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
