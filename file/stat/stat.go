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

	lnMu  sync.Mutex
	bMu   sync.Mutex
	chSMu sync.Mutex
}

// AddLine will atomically and safely increment
// LineCnt by one.
func (s *Stat) AddLine() {
	s.lnMu.Lock()
	defer s.lnMu.Unlock()

	atomic.AddInt64(&s.LineCnt, 1)
}

// AddBytes will atomically and safely increment
// ByteCnt by 'cnt'.
func (s *Stat) AddBytes(cnt int64) {
	s.lnMu.Lock()
	defer s.lnMu.Unlock()

	atomic.AddInt64(&s.ByteCnt, 1)
}

// SetCheckSum will correctly calculate and set the
// base64 encoded checksum.
func (s *Stat) SetCheckSum(hsh hash.Hash) {
	s.chSMu.Lock()
	defer s.chSMu.Unlock()

	s.CheckSum = hex.EncodeToString(hsh.Sum(nil))
}

func (s *Stat) SetSizeFromPath(pth string) {
	fInfo, err := os.Stat(pth)
	if err != nil {
		return
	}
	s.Size = fInfo.Size()
}

func (s *Stat) SetSize(size int64) {
	s.Size = size
}

// Clone will safely clone the Stat object.
func (s *Stat) Clone() Stat {
	s.lnMu.Lock()
	s.bMu.Lock()
	s.chSMu.Lock()
	defer s.lnMu.Unlock()
	defer s.bMu.Unlock()
	defer s.chSMu.Unlock()

	return *s
}
